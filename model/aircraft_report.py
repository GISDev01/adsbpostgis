"""
Aircraft report defined as a record from the mode-s BEAST feed.
Each report is sent to Kafka for ingestion into Postgres
"""

import json
import logging
import time

import requests

from utils import mathutils

logger = logging.getLogger(__name__)

KNOTS_TO_KMH = 1.852
FEET_TO_METERS = 0.3048

reporter_format = "{:10.10}"
flight_format = "{:8.8}"

"""
Partial original implementation of this class pulled from this repo: 
https://github.com/stephen-hocking/ads-b-logger
"""
# A number of different implementations of dump1090 exist,
# offering varying amounts of info from the auto-updating data.json
# The dump1090mutable has a far richer json interface, where the planes are
# found via http://localhost:8080/data/aircraft.json, which is itself
# a multilevel JSON document.
DUMP1090_MIN = ["hex", "lat", "lon", "altitude", "track", "speed"]
DUMP1090_ANTIREZ = DUMP1090_MIN + ["flight"]
DUMP1090_MALROBB = DUMP1090_ANTIREZ + ["squawk", "validposition", "vert_rate",
                                       "validtrack", "messages", "seen"]
DUMP1090_PIAWARE = DUMP1090_MALROBB + ["mlat"]

MUTABLE_EXTRAS = ["nucp", "seen_pos", "category", "rssi"]

DUMP1090_FULLMUT = DUMP1090_MALROBB + MUTABLE_EXTRAS
DUMP1090_MINMUT = ["hex", "rssi", "seen"]

# The mutable branch has variable members in each aircraft list.
MUTABLE_TRYLIST = list(set(DUMP1090_FULLMUT) - set(DUMP1090_MINMUT))
DUMP1090_DBADD = ["isMetric", "time", "reporter", "isGnd", "report_location"]
DUMP1090_DBLIST = list(set(DUMP1090_PIAWARE + DUMP1090_DBADD) - set(["seen"]))
DUMP1090_FULL = DUMP1090_FULLMUT + DUMP1090_DBADD

VRS_KEYWORDS = ["PosTime", "Icao", "Alt", "Spd", "Sqk", "Trak", "Long", "Lat", "Gnd",
                "CMsgs", "Mlat"]
VRSFILE_KEYWORDS = VRS_KEYWORDS + ["Cos", "TT"]


class AircraftReport(object):
    """
    Aircraft position reports, from the data.json interface of dump1090
    Creates objects from JSON structures either from dump1090 JSON, a static dump file, or a database connection
    """

    # Set all of these initially outside the self/object scope as defaults, and then we set them properly within
    # the init, if they exist in the JSON that is being parsed within the object
    hex = None
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

    def __init__(self, **kwargs):
        # Dynamic unpacking of the object's input JSON, since we need to support various formats with
        # many possibilities of which dict keys exist or don't exist, so we loop through and check them all
        for keyword in DUMP1090_FULL:
            try:
                setattr(self, keyword, kwargs[keyword])
            except KeyError:
                pass

        if not self.is_metric:
            self.convert_to_metric()

        is_ground = getattr(self, 'isGnd', None)
        if is_ground is None:
            if self.altitude == 0:
                setattr(self, 'isGnd', True)
            else:
                setattr(self, 'isGnd', False)

        multi_lat = getattr(self, 'mlat', None)
        if multi_lat is None:
            setattr(self, 'mlat', False)

        signal_strength = getattr(self, 'rssi', None)
        if signal_strength is None:
            setattr(self, 'rssi', None)

        is_nucp = getattr(self, 'nucp', None)
        if is_nucp is None:
            setattr(self, 'nucp', -1)

    def convert_to_metric(self):
        """Converts aircraft report to use metric units"""
        self.vert_rate = self.vert_rate * FEET_TO_METERS
        self.altitude = int(self.altitude * FEET_TO_METERS)
        self.speed = int(self.speed * KNOTS_TO_KMH)
        self.is_metric = True

    def convert_from_metric_to_us(self):
        """Converts aircraft report to knots/feet"""
        self.vert_rate = self.vert_rate / FEET_TO_METERS
        self.altitude = int(self.altitude / FEET_TO_METERS)
        self.speed = int(self.speed / KNOTS_TO_KMH)
        self.is_metric = False

    def __str__(self):
        fields = ['  {}: {}'.format(k, v) for k, v in self.__dict__.items()
                  if not k.startswith("_")]
        return "{}(\n{})".format(self.__class__.__name__, '\n'.join(fields))

    # Creates a representation that can be loaded from a single text line
    def to_json(self):
        """Returns a JSON representation of an aircraft report on one line"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, separators=(',', ':'))

    # Log this report to the database
    def send_aircraft_to_db(self, database_connection, print_query=False, update=False):
        """
        Send this JSON record into the DB in an open connection
        :type print_query: bool
        :param database_connection: Open database connection
        :param print_query: Debugging mode to see the data getting inserted into the DB
        :param update: 
        :return: 
        """

        # Need to extract datetime fields from time
        # Need to encode lat/lon appropriately
        cur = database_connection.cursor()
        coordinates = "POINT(%s %s)" % (self.lon, self.lat)
        if update:
            params = [self.hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.lat, self.lon,
                      self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.isGnd,
                      self.hex, self.squawk, flight_format.format(self.flight),
                      reporter_format.format(self.reporter), self.time, self.messages]
            # TODO: Refactor with proper ORM to avoid SQLi vulns
            sql = '''UPDATE aircraftreports SET (hex, squawk, flight, is_metric, is_MLAT, altitude, speed, vert_rate, bearing, report_location, latitude83, longitude83, messages_sent, report_epoch, reporter, rssi, nucp, is_ground)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s)
            WHERE hex like %s and squawk like %s and flight like %s and reporter like %s
            and report_epoch = %s and messages_sent = %s'''

        else:
            params = [self.hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.lat, self.lon,
                      self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.isGnd]
            sql = '''INSERT into aircraftreports (hex, squawk, flight, is_metric, is_MLAT, altitude, speed, vert_rate, bearing, report_location, latitude83, longitude83, messages_sent, report_epoch, reporter, rssi, nucp, is_ground)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s);'''

        logger.info(cur.mogrify(sql, params))
        cur.execute(sql, params)
        cur.close()

    # Delete record - assuming sampling once a second, the combination of
    # hex, report_epoch and reporter should be unique
    def delete_from_db(self, db_connection):
        """
        Deletes the record that matches the plane report from the DB

        Args:
            db_connection: An existing DB connection

        Returns:
            Nothing much

        Raises:
            psycopg2 exceptions
        """
        cur = db_connection.cursor()
        sql = '''DELETE from aircraftreports WHERE '''
        sql = sql + (" hex like '%s' " % self.hex)
        sql = sql + (" and flight like '%s' " % flight_format.format(self.flight))
        sql = sql + (" and reporter like '%s'" %
                     reporter_format.format(self.reporter))
        sql = sql + (" and report_epoch=%s " % self.time)
        sql = sql + (" and altitude=%s " % self.altitude)
        sql = sql + (" and speed=%s " % self.speed)
        sql = sql + (" and messages_sent=%s" % self.messages)

        logger.info(cur.mogrify(sql))
        cur.execute(sql)

    # Distance from another object with lat/lon
    def distance(self, other_location):
        """Returns distance in meters from another object with lat/lon"""
        return mathutils.haversine_distance_meters(self.lon, self.lat, other_location.lon, other_location.lat)


def get_aircraft_data_from_url(url_string, url_params=None):
    """
    Reads JSON objects from a server at a URL (usually a dump1090 instance)

    Args:
        url_string: A string containing a URL (e.g. http://mydump1090:8080/data.json)
        url_params: parameters used for filtering requests to adsbexchange.com

    Returns:
        A list of AircraftReports
    """
    current_report_pulled_time = time.time()
    if url_params:
        response = requests.get(url_string, params=url_params)
    else:
        response = requests.get(url_string)
    data = json.loads(response.text)

    # Check for dump1090 JSON Schema (should contain array of report within an aircraft key)
    if 'aircraft' in data:
        reports_list = ingest_dump1090_report_list(data['aircraft'])

    # VRS style - adsbexchange.com
    elif 'acList' in data:
        reports_list = []
        for vrs_report in data['acList']:
            vrs_aircraft_report_parsed = ingest_vrs_format_record(vrs_report, current_report_pulled_time)
            reports_list.append(vrs_aircraft_report_parsed)

    else:
        reports_list = [AircraftReport(**pl) for pl in data]
    return reports_list


def load_aircraft_reports_list_into_db(aircraft_reports_list, radio_receiver, dbconn):
    current_timestamp = int(time.time())
    for aircraft in aircraft_reports_list:
        if aircraft.validposition and aircraft.validtrack:
            aircraft.time = current_timestamp - aircraft.seen
            aircraft.reporter = radio_receiver.name
            if dbconn:
                aircraft.send_aircraft_to_db(dbconn)
            else:
                logger.info(aircraft)
        else:
            logger.error("Dropped report " + aircraft.to_JSON())
    if dbconn:
        dbconn.commit()


def ingest_vrs_format_record(vrs_aircraft_report, report_pulled_timestamp):
    logger.debug('Ingest VRS Format')
    valid = True
    for key_name in VRS_KEYWORDS:
        if key_name not in vrs_aircraft_report:
            valid = False
            break
    if valid:
        report_position_time = vrs_aircraft_report['PosTime'] / 1000
        hex = vrs_aircraft_report['Icao'].lower()
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
        isGnd = vrs_aircraft_report['Gnd']
        messages = vrs_aircraft_report['CMsgs']
        mlat = vrs_aircraft_report['Mlat']

        if 'Vsi' in vrs_aircraft_report:
            vert_rate = vrs_aircraft_report['Vsi']
        else:
            vert_rate = 0.0
        is_metric = False
        seen = seen_pos = (report_pulled_timestamp - report_position_time)
        plane = AircraftReport(hex=hex, time=report_position_time, speed=speed, squawk=squawk, flight=flight,
                               altitude=altitude, isMetric=is_metric,
                               track=track, lon=lon, lat=lat, vert_rate=vert_rate, seen=seen,
                               validposition=1, validtrack=1, reporter="", mlat=mlat, isGnd=isGnd,
                               report_location=None, messages=messages, seen_pos=seen_pos, category=None)

        return plane


def ingest_dump1090_report_list(dumpfmt_aircraft_report):
    dump1090_ingested_reports_list = []
    for dumpfmt_aircraft_report in dumpfmt_aircraft_report:
        logger.debug('Ingest Dump1090 Format')

        valid = True
        for key_name in DUMP1090_MIN:
            if key_name not in dumpfmt_aircraft_report:
                valid = False
                break
        if valid:
            if dumpfmt_aircraft_report['altitude'] == 'ground':
                dumpfmt_aircraft_report['altitude'] = 0
                dump1090_aircraft_report = AircraftReport(**dumpfmt_aircraft_report)
                setattr(dump1090_aircraft_report, 'isGnd', True)
            else:
                dump1090_aircraft_report = AircraftReport(**dumpfmt_aircraft_report)
                setattr(dump1090_aircraft_report, 'isGnd', False)
            setattr(dump1090_aircraft_report, 'validposition', 1)
            setattr(dump1090_aircraft_report, 'validtrack', 1)

            # mutability dump1090 has mlat set to list of attributes mlat'ed, we want a boolean
            if 'mlat' not in dumpfmt_aircraft_report:
                setattr(dump1090_aircraft_report, 'mlat', False)
            else:
                setattr(dump1090_aircraft_report, 'mlat', True)

            logger.info(dump1090_aircraft_report.to_json())

            dump1090_ingested_reports_list.append(dump1090_aircraft_report)

        else:
            logger.debug('Skipping this invalid Dump1090 report: ' + json.dumps(dumpfmt_aircraft_report))

    return dump1090_ingested_reports_list
