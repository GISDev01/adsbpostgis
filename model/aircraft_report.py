"""
Aircraft report defined as a record from the mode-s BEAST feed
Each report is sent to Kafka for ingestion into Postgres/PostGIS and a Storm Spout
"""

import json
import logging
import os
import time

# TODO: Refactor the DB connection into a param, so there's not need to import main here
import main

import requests

from utils import mathutils

from model import report_receiver

logger = logging.getLogger(__name__)

knots_to_kmh = 1.852
ft_to_meters = 0.3048
reporter_format = "{:10.10}"
flight_format = "{:8.8}"

# A number of different implementations of dump1090 exist,
# offering varying amounts of info from the auto-updating data.json
# The dump1090mutable has a far richer json interface, where the planes are
# found via http://localhost:8080/data/aircraft.json, which is itself
# a multilevel JSON document.
dump1090_minimum_keynames = ["hex", "lat", "lon", "altitude", "track", "speed"]
dump1090_antirez_keynames = dump1090_minimum_keynames + ["flight"]
dump1090_malrobb_keynames = dump1090_antirez_keynames + ["squawk", "validposition", "vert_rate",
                                                         "validtrack", "messages", "seen"]
dump1090_piaware_keynames = dump1090_malrobb_keynames + ["mlat"]

mutable_extra_keynames = ["nucp", "seen_pos", "category", "rssi"]

dump1090_full_mutable_keynames = dump1090_malrobb_keynames + mutable_extra_keynames
dump1090_minimum_mutable_keynames = ["hex", "rssi", "seen"]

# The mutable branch has variable members in each aircraft list.
mutable_keynames_try = list(set(dump1090_full_mutable_keynames) - set(dump1090_minimum_mutable_keynames))
dump1090_database_add_keynames = ["isMetric", "time", "reporter", "is_ground", "report_location"]
dump1090_database_keynames = list(set(dump1090_piaware_keynames + dump1090_database_add_keynames) - set(["seen"]))
dump1090_all_keynames = dump1090_full_mutable_keynames + dump1090_database_add_keynames

adsb_vrs_keynames = ["PosTime", "Icao", "Alt", "Spd", "Sqk", "Trak", "Long", "Lat", "Gnd",
                     "CMsgs", "Mlat"]
vrs_adsb_file_keynames = adsb_vrs_keynames + ["Cos", "TT"]

"""
Partial original implementation of this class pulled from this repo: 
https://github.com/stephen-hocking/ads-b-logger
"""


class AircraftReport(object):
    """
    Aircraft position reports, from the data.json interface of dump1090
    Creates objects from JSON structures either from dump1090 JSON, a static dump file, or a database connection
    """

    # Set all of these initially outside the self/object scope as defaults, and then we set them properly within
    # the init, if they exist in the JSON that is being parsed within the object
    mode_s_hex = None
    altitude = 0.0
    speed = 0.0
    squawk = None
    flight = None
    track = 0
    lon = 0.0
    lat = 0.0
    vert_rate = 0.0
    seen = 9999999
    valid_position = 1
    valid_track = 1
    time = 0
    reporter = None
    report_location = None
    is_metric = False
    messages = 0
    seen_pos = -1
    category = None
    is_anon = None
    is_ground = None
    mlat = None
    rssi = None
    nucp = None

    def __init__(self, **kwargs):
        # Dynamic unpacking of the object's input JSON, since we need to support various formats with
        # many possibilities of which dict keys exist or don't exist, so we loop through and check them all
        for keyword in dump1090_all_keynames:
            try:
                setattr(self, keyword, kwargs[keyword])
            except KeyError:
                pass

        if not self.is_metric:
            self.convert_to_metric()

        _is_ground = getattr(self, 'is_ground', None)
        if _is_ground is None:
            if self.altitude == 0:
                setattr(self, 'is_ground', True)
            else:
                setattr(self, 'is_ground', False)

        _multi_lat = getattr(self, 'mlat', None)
        if _multi_lat is None:
            setattr(self, 'mlat', False)

        _signal_strength = getattr(self, 'rssi', None)
        if _signal_strength is None:
            setattr(self, 'rssi', None)

        _is_nucp = getattr(self, 'nucp', None)
        if _is_nucp is None:
            setattr(self, 'nucp', -1)

        _hex = getattr(self, 'hex', None)
        if _hex is not None:
            setattr(self, 'mode_s_hex', _hex.upper())

        # FA anonymizes the mode-s hex for certain aircraft, and denotes it with a ~ as
        # the first character in the fake mode-s hex code they send back on the MLAT results
        if _hex[0] == '~':
            setattr(self, 'is_anon', True)
            self.process_anon_detection()
        else:
            setattr(self, 'is_anon', False)

    def convert_to_metric(self):
        """Converts aircraft report to use metric units"""
        self.vert_rate = self.vert_rate * ft_to_meters
        self.altitude = int(self.altitude * ft_to_meters)
        self.speed = int(self.speed * knots_to_kmh)
        self.is_metric = True

    def convert_from_metric_to_us(self):
        """Converts aircraft report to use Freedom units"""
        self.vert_rate = self.vert_rate / ft_to_meters
        self.altitude = int(self.altitude / ft_to_meters)
        self.speed = int(self.speed / knots_to_kmh)
        self.is_metric = False

    def __str__(self):
        fields = ['  {}: {}'.format(k, v) for k, v in self.__dict__.items()
                  if not k.startswith("_")]
        return "{}(\n{})".format(self.__class__.__name__, '\n'.join(fields))

    def to_json(self):
        """Returns a JSON representation of an aircraft report on one line"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, separators=(',', ':'))

    def send_aircraft_to_db(self, database_connection, update=False):
        """
        Send this JSON record into the DB in an open connection
        
        :param database_connection: Open database connection
        :param update: bool to indicate an update or insert (TODO: use upsert)
        :return: None
        """

        # Need to extract datetime fields from time
        # Need to encode lat/lon appropriately for PostGIS storage (spatially indexed)
        cur = database_connection.cursor()
        coordinates = "POINT(%s %s)" % (self.lon, self.lat)
        if update:
            params = [self.mode_s_hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.lat, self.lon,
                      self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.is_ground,
                      self.mode_s_hex, self.squawk, flight_format.format(self.flight),
                      reporter_format.format(self.reporter), self.time, self.messages, self.is_anon]
            # TODO: Refactor with proper ORM to avoid SQLi vulns
            sql = '''UPDATE aircraftreports SET (mode_s_hex, squawk, flight, is_metric, is_mlat, altitude, speed, vert_rate, bearing, report_location, latitude83, longitude83, messages_sent, report_epoch, reporter, rssi, nucp, is_ground, is_anon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            WHERE mode_s_hex like %s and squawk like %s and flight like %s and reporter like %s
            and report_epoch = %s and messages_sent = %s'''

        else:
            params = [self.mode_s_hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.lat, self.lon,
                      self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.is_ground, self.is_anon]
            sql = '''INSERT into aircraftreports (mode_s_hex, squawk, flight, is_metric, is_mlat, altitude, speed, vert_rate, bearing, report_location, latitude83, longitude83, messages_sent, report_epoch, reporter, rssi, nucp, is_ground, is_anon)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

        logger.debug(cur.mogrify(sql, params))
        cur.execute(sql, params)
        cur.close()

    def delete_from_db(self, db_connection):
        """
        Delete a record that matches this object - assuming sampling once a second, the combination of
        mode_s_hex, report_epoch, and reporter should be unique
        
        :param db_connection: Open database connection
        :return: 
        """
        cur = db_connection.cursor()
        sql = '''DELETE from aircraftreports WHERE '''
        sql = sql + (" mode_s_hex like '%s' " % self.mode_s_hex)
        sql = sql + (" and flight like '%s' " % flight_format.format(self.flight))
        sql = sql + (" and reporter like '%s'" %
                     reporter_format.format(self.reporter))
        sql = sql + (" and report_epoch=%s " % self.time)
        sql = sql + (" and altitude=%s " % self.altitude)
        sql = sql + (" and speed=%s " % self.speed)
        sql = sql + (" and messages_sent=%s" % self.messages)

        logger.debug(cur.mogrify(sql))
        cur.execute(sql)

    def distance(self, other_location):
        """Returns distance in meters from another object with lat/lon"""
        return mathutils.haversine_distance_meters(self.lon, self.lat, other_location.lon, other_location.lat)

    def process_anon_detection(self):
        logger.warning('Anon Mode S Hex detected: {} - Location: {}/{}'.format(self.hex, self.lat, self.lon))
        # pass
        # adsbe_params = {
        #     'fNBnd': 33.94290171650591,
        #     'fEBnd': -97.09046957492819,
        #     'fSBnd': 33.82557994879984,
        #     'fWBnd': -97.33457205295554,
        #     'trFmt': 'fa'
        # }


def get_aircraft_data_from_url(url_string, url_params=None):
    """
    :param url_string: string containing a URL (e.g. http://piaware1/dump1090-fa/data.json)
    :param url_params: Only used for ADSBE data pulls
    :return: list of AircraftReport objects
    """
    current_report_pulled_time = time.time()

    if url_params:
        response = requests.get(url_string, params=url_params)
    else:
        response = requests.get(url_string)
    try:
        data = json.loads(response.text)
    except:
        logger.warning('Unable to parse the aircraft JSON from dump1090')
        return []
    # Check for dump1090 JSON Schema (should contain a list of reports with an aircraft key in the JSON)
    if 'aircraft' in data:
        reports_list = ingest_dump1090_report_list(data['aircraft'])

    # VRS style JSON Schema - such as the JSON from adsbexchange.com
    elif 'acList' in data:
        reports_list = []
        for vrs_report in data['acList']:
            vrs_aircraft_report_parsed = ingest_vrs_format_record(vrs_report, current_report_pulled_time)
            reports_list.append(vrs_aircraft_report_parsed)

    else:
        # Wildcard format so we just load each JSON key directly into each AircraftReport object
        reports_list = [AircraftReport(**pl) for pl in data]

    return reports_list


def get_aircraft_data_from_files(file_directory):
    """
    Sample record:
    {"acList":[
{"Id":10981358,"Rcvr":1,"HasSig":true,"Sig":31,"Icao":"A78FEE","Bad":false,"FSeen":"\/Date(1492992031138)\/","CMsgs":1,"AltT":0,"Tisb":false,"TrkH":false,"Sqk":"1200","Help":false,"VsiT":0,"WTC":0,"Species":0,"EngType":0,"EngMount":0,"Mil":false,"Cou":"United States","HasPic":false,"Interested":false,"FlightsCount":0,"Gnd":true,"SpdTyp":0,"CallSus":false,"TT":"a","Trt":1}, ... ]}
    Args:
        file_directory: A string containing a filepath

    Returns:
        A list of AircraftReports
    """
    radio_receiver_vrs = report_receiver.RadioReceiver(name='archive',
                                                       type='vrs',
                                                       lat83=0,
                                                       long83=0,
                                                       data_access_url='',
                                                       location='')

    files_to_process = []
    malformed_json_files = []

    for file in os.listdir(file_directory):
        if file.endswith('.json'):
            logger.info('Found Aircraft JSON data file: {}'.format(os.path.join(file_directory, file)))
            files_to_process.append(os.path.join(file_directory, file))

    for json_file in files_to_process:
        logger.info('Processing Aircraft JSON data file: {} '.format(json_file))

        file_report_list = []
        try:
            file_data = json.load(open(json_file, encoding='utf-8'))
        except:
            logger.error('Error parsing JSON data file: {}'.format(json_file))
            malformed_json_files.append(json_file)
            pass

        for aircraft_record in file_data['acList']:
            valid = True
            for json_key_name in vrs_adsb_file_keynames:
                if json_key_name not in aircraft_record:
                    valid = False
                    break
            if valid:
                report_time = aircraft_record['PosTime'] / 1000
                mode_s_hex = aircraft_record['Icao'].upper()
                altitude = aircraft_record['Alt']
                speed = aircraft_record['Spd']
                squawk = aircraft_record['Sqk']
                if 'Call' in aircraft_record:
                    flight = flight_format.format(aircraft_record['Call'])
                else:
                    flight = ''
                track = aircraft_record['Trak']
                long83 = aircraft_record['Long']
                lat83 = aircraft_record['Lat']

                is_ground = aircraft_record['Gnd']
                messages = aircraft_record['CMsgs']
                mlat = aircraft_record['Mlat']
                tt = aircraft_record['TT']

                if 'Vsi' in aircraft_record:
                    vert_rate = aircraft_record['Vsi']
                else:
                    vert_rate = 0.0
                is_metric = False

                # Calculate each position in the past track data and insert as an Aircraft record
                past_track = aircraft_record['Cos']
                if tt == 'a' or tt == 's':
                    numpos = len(past_track) / 4
                    for i in range(int(numpos)):
                        if past_track[(i * 4) + 3]:
                            if tt == 'a':
                                altitude = past_track[(i * 4) + 3]
                            elif tt == 's':
                                speed = past_track[(i * 4) + 3]
                            lat83 = past_track[(i * 4) + 0]
                            long83 = past_track[(i * 4) + 1]
                            if lat83 < -90.0 or lat83 > 90.0 or long83 < -180.0 or long83 > 180.0:
                                continue
                            report_time = past_track[(i * 4) + 2] / 1000
                            seen = seen_pos = 0
                            record = AircraftReport(hex=mode_s_hex,
                                                    time=report_time,
                                                    speed=speed,
                                                    squawk=squawk,
                                                    flight=flight,
                                                    altitude=altitude,
                                                    isMetric=is_metric,
                                                    track=track,
                                                    lon=long83,
                                                    lat=lat83,
                                                    vert_rate=vert_rate,
                                                    seen=seen,
                                                    validposition=1,
                                                    validtrack=1,
                                                    reporter="",
                                                    mlat=mlat,
                                                    is_ground=is_ground,
                                                    report_location=None,
                                                    messages=messages,
                                                    seen_pos=seen_pos,
                                                    category=None)
                            file_report_list.append(record)

                            load_aircraft_reports_list_into_db(aircraft_reports_list=file_report_list,
                                                               radio_receiver=radio_receiver_vrs,
                                                               dbconn=main.postgres_db_connection)

    logger.info('Malformed JSON Files found: {}'.format(malformed_json_files))


def load_aircraft_reports_list_into_db(aircraft_reports_list, radio_receiver, dbconn):
    current_timestamp = int(time.time())
    for aircraft in aircraft_reports_list:
        if aircraft.validposition and aircraft.validtrack:
            aircraft.time = current_timestamp - aircraft.seen
            aircraft.reporter = radio_receiver.name
            if dbconn:
                aircraft.send_aircraft_to_db(dbconn)
            else:
                logger.warning('No DB Connection. Aircraft not inserted; {}'.format(aircraft))
        else:
            logger.error("Dropped report " + aircraft.to_JSON())
    if dbconn:
        dbconn.commit()


def ingest_vrs_format_record(vrs_aircraft_report, report_pulled_timestamp):
    logger.info('Ingesting VRS Format Record')
    valid = True
    for key_name in adsb_vrs_keynames:
        if key_name not in vrs_aircraft_report:
            valid = False
            logger.exception('VRS Record key is invalid: {}'.format(key_name))
            break
    if valid:
        report_position_time = vrs_aircraft_report['PosTime'] / 1000
        hex = vrs_aircraft_report['Icao'].upper()
        altitude = vrs_aircraft_report['Alt']
        speed = vrs_aircraft_report['Spd']
        squawk = vrs_aircraft_report['Sqk']
        if 'Call' in vrs_aircraft_report:
            flight = flight_format.format(vrs_aircraft_report['Call'])
        else:
            flight = ' '
        track = vrs_aircraft_report['Trak']
        lon = vrs_aircraft_report['Long']
        lat = vrs_aircraft_report['Lat']
        is_ground = vrs_aircraft_report['Gnd']
        messages = vrs_aircraft_report['CMsgs']
        mlat = vrs_aircraft_report['Mlat']

        if 'Vsi' in vrs_aircraft_report:
            vert_rate = vrs_aircraft_report['Vsi']
        else:
            vert_rate = 0.0
        is_metric = False
        if report_pulled_timestamp is not None:
            seen = seen_pos = (report_pulled_timestamp - report_position_time)
        else:
            seen_pos = 0
        plane = AircraftReport(hex=hex,
                               time=report_position_time,
                               speed=speed,
                               squawk=squawk,
                               flight=flight,
                               altitude=altitude,
                               isMetric=is_metric,
                               track=track,
                               lon=lon,
                               lat=lat,
                               vert_rate=vert_rate,
                               seen=seen,
                               validposition=1,
                               validtrack=1,
                               reporter="",
                               mlat=mlat,
                               is_ground=is_ground,
                               report_location=None,
                               messages=messages,
                               seen_pos=seen_pos,
                               category=None)

        return plane


def ingest_dump1090_report_list(dumpfmt_aircraft_report_list):
    dump1090_ingested_reports_list = []
    for dumpfmt_aircraft_report in dumpfmt_aircraft_report_list:
        logger.debug('Ingest Dump1090 Format')

        valid = True
        for key_name in dump1090_minimum_keynames:
            if key_name not in dumpfmt_aircraft_report:
                valid = False
                break
        if valid:
            if dumpfmt_aircraft_report['altitude'] == 'ground':
                dumpfmt_aircraft_report['altitude'] = 0
                dump1090_aircraft_report = AircraftReport(**dumpfmt_aircraft_report)
                setattr(dump1090_aircraft_report, 'is_ground', True)
            else:
                dump1090_aircraft_report = AircraftReport(**dumpfmt_aircraft_report)
                setattr(dump1090_aircraft_report, 'is_ground', False)
            setattr(dump1090_aircraft_report, 'validposition', 1)
            setattr(dump1090_aircraft_report, 'validtrack', 1)

            # mutability dump1090 has mlat set to list of attributes mlat'ed, we want a boolean
            if 'mlat' not in dumpfmt_aircraft_report:
                setattr(dump1090_aircraft_report, 'mlat', False)
            else:
                setattr(dump1090_aircraft_report, 'mlat', True)

            setattr(dump1090_aircraft_report, 'mode_s_hex', dumpfmt_aircraft_report['hex'])
            logger.debug(dump1090_aircraft_report.to_json())

            dump1090_ingested_reports_list.append(dump1090_aircraft_report)

        else:
            logger.debug('Skipping this invalid Dump1090 report: ' + json.dumps(dumpfmt_aircraft_report_list))

    return dump1090_ingested_reports_list
