"""
Aircraft report defined as a record from the mode-s BEAST feed.
Each report is sent to Kafka for ingestion into Postgres
"""

import json
import time

import psycopg2
import requests
import yaml

import logging

from utils import mathutils

logger = logging.getLogger(__name__)

KNOTS_TO_KMH = 1.852
FEET_TO_METERS = 0.3048

RPTR_FMT = "{:10.10}"
FLT_FMT = "{:8.8}"

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

        # signal_strength = getattr(self, 'rssi', None)
        # if signal_strength is None:
        #     setattr(self, 'rssi', None)

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

    # Make it easier to log this object
    def __str__(self):
        fields = ['  {}: {}'.format(k, v) for k, v in self.__dict__.iteritems()
                  if not k.startswith("_")]
        return "{}(\n{})".format(self.__class__.__name__, '\n'.join(fields))

    # Creates a representation that can be loaded from a single text line
    def to_json(self):
        """Returns a JSON representation of an aircraft report on one line"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, separators=(',', ':'))

    # Log this report to the database
    def send_to_db(self, database_connection, print_query=False, update=False):
        """
        Send this JSON record into the DB in an open connection
        :type print_query: bool
        :param database_connection: Open database connection
        :param print_query: Debugging mode to see the data getting inserted into the DB
        :param update: 
        :return: 
        """
        #
        # Need to extract datetime fields from time
        # Need to encode lat/lon appropriately
        cur = database_connection.cursor()
        coordinates = "POINT(%s %s)" % (self.lon, self.lat)
        if update:
            params = [self.hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.isGnd,
                      self.hex, self.squawk, FLT_FMT.format(self.flight),
                      RPTR_FMT.format(self.reporter), self.time, self.messages]
            # TODO: Refactor with proper ORM to avoid SQLi vulns
            sql = '''
        UPDATE planereports SET (hex, squawk, flight, "isMetric", "isMLAT", altitude, speed, vert_rate, bearing, report_location, messages_sent, report_epoch, reporter, rssi, nucp, isgnd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326),
            %s, %s, %s, %s, %s, %s)
            WHERE hex like %s and squawk like %s and flight like %s and reporter like %s
            and report_epoch = %s and messages_sent = %s'''

        else:
            params = [self.hex, self.squawk, self.flight, self.is_metric,
                      self.mlat, self.altitude, self.speed, self.vert_rate,
                      self.track, coordinates, self.messages, self.time, self.reporter,
                      self.rssi, self.nucp, self.isGnd]
            sql = '''
        INSERT into planereports (hex, squawk, flight, "isMetric", "isMLAT", altitude, speed, vert_rate, bearing, report_location, messages_sent, report_epoch, reporter, rssi, nucp, isgnd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s, 4326), %s, %s, %s, %s, %s, %s);'''

        if print_query:
            print(cur.mogrify(sql, params))

        cur.execute(sql, params)
        cur.close()

    # Delete record - assuming sampling once a second, the combination of
    # hex, report_epoch and reporter should be unique
    def delete_from_db(self, dbconn, printQuery=None):
        """
        Deletes the record that matches the plane report from the DB

        Args:
            dbconn: An existing DB connection
            printQuery:  A boolean which controls the printing of the query

        Returns:
            Nothing much

        Raises:
            psycopg2 exceptions
        """
        cur = dbconn.cursor()
        sql = '''DELETE from planereports WHERE '''
        sql = sql + (" hex like '%s' " % self.hex)
        sql = sql + (" and flight like '%s' " % FLT_FMT.format(self.flight))
        sql = sql + (" and reporter like '%s'" %
                     RPTR_FMT.format(self.reporter))
        sql = sql + (" and report_epoch=%s " % self.time)
        sql = sql + (" and altitude=%s " % self.altitude)
        sql = sql + (" and speed=%s " % self.speed)
        sql = sql + (" and messages_sent=%s" % self.messages)
        if printQuery:
            print(cur.mogrify(sql))
        cur.execute(sql)

    # Distance from another object with lat/lon
    def distance(self, reporter):
        """Returns distance in meters from another object with lat/lon"""
        return mathutils.haversine_distance_meters(self.lon, self.lat, reporter.lon, reporter.lat)


def database_connection(yamlfile, dbuser=None, dbhost=None, dbpasswd=None, dbport=5432):
    """
    Makes a connection to a Postgres DB, dictated by a yaml file, with optional
    overides.

    Args:
        yamlfile: Filename of the yaml file (compulsory)
        dbuser: Contains name of database user to login (optional)
        dbhost: Name of host that DB is running on (optional)
        dbpasswd: Password for the DB account (optional)
        dbport: Portnumber to connect to on DB host (optional)

    Returns:
        psycopg2 DB connection

    Could do with some sprucing up. Format of yamlfile looks like:
        adsb_logger:
            dbhost: somehost.somewhere.com
            dbuser: some_username
            dbpassword: S3kr1t_P4ssw0rd
    """
    dbconn = None

    # Also allow yaml file to be completely or partially overridden
    if not (dbuser and dbhost and dbpasswd):
        with open(yamlfile, 'r') as db_cfg_file:
            db_conf = yaml.load(db_cfg_file)
        if not dbhost:
            dbhost = db_conf["adsb_logger"]["dbhost"]
        if not dbuser:
            dbuser = db_conf["adsb_logger"]["dbuser"]
        if not dbpasswd:
            dbpasswd = db_conf["adsb_logger"]["dbpassword"]
    connect_str = "dbname=PlaneReports user=" + dbuser + " host=" + \
                  dbhost + " password=" + dbpasswd + " port=" + str(dbport)
    try:
        dbconn = psycopg2.connect(connect_str)
    except:
        print("Can't connect to plane report database with " + connect_str)
        exit(-1)
    return dbconn


def get_aircraft_data_from_url(url_string, url_params=None):
    """
    Reads JSON objects from a server at a URL (usually a dump1090 instance)

    Args:
        url_string: A string containing a URL (e.g. http://mydump1090:8080/data.json)
        url_params: parameters used for filtering requests to adsbexchange.com

    Returns:
        A list of PlaneReports
    """
    cur_time = time.time()
    if url_params:
        response = requests.get(url_string, params=url_params)
    else:
        response = requests.get(url_string)
    data = json.loads(response.text)

    # Check for dump1090_mutability style of interface
    if 'aircraft' in data:
        reports_list = []
        for pl in data['aircraft']:
            valid = True
            for keywrd in DUMP1090_MIN:
                if keywrd not in pl:
                    valid = False
                    break
            if valid:
                if pl['altitude'] == 'ground':
                    pl['altitude'] = 0
                    plane = AircraftReport(**pl)
                    setattr(plane, 'isGnd', True)
                else:
                    plane = AircraftReport(**pl)
                    setattr(plane, 'isGnd', False)
                setattr(plane, 'validposition', 1)
                setattr(plane, 'validtrack', 1)

                # mutability has mlat set to list of attrs mlat'ed - we want bool
                if 'mlat' not in pl:
                    setattr(plane, 'mlat', False)
                else:
                    setattr(plane, 'mlat', True)

                logger.debug(plane.to_json())
                reports_list.append(plane)

    # VRS style - adsbexchange.com        
    elif 'acList' in data:
        reports_list = []
        for pl in data['acList']:
            valid = True
            for keywrd in VRS_KEYWORDS:
                if keywrd not in pl:
                    valid = False
                    break
            if valid:
                mytime = pl['PosTime'] / 1000
                hex = pl['Icao'].lower()
                altitude = pl['Alt']
                speed = pl['Spd']
                squawk = pl['Sqk']
                if 'Call' in pl:
                    flight = FLT_FMT.format(pl['Call'])
                else:
                    flight = ' '
                track = pl['Trak']
                lon = pl['Long']
                lat = pl['Lat']
                isGnd = pl['Gnd']
                messages = pl['CMsgs']
                mlat = pl['Mlat']

                if 'Vsi' in pl:
                    vert_rate = pl['Vsi']
                else:
                    vert_rate = 0.0
                isMetric = False
                seen = seen_pos = (cur_time - mytime)
                plane = AircraftReport(hex=hex, time=mytime, speed=speed, squawk=squawk, flight=flight,
                                       altitude=altitude, isMetric=False,
                                       track=track, lon=lon, lat=lat, vert_rate=vert_rate, seen=seen,
                                       validposition=1, validtrack=1, reporter="", mlat=mlat, isGnd=isGnd,
                                       report_location=None, messages=messages, seen_pos=seen_pos, category=None)
                reports_list.append(plane)

    else:
        reports_list = [AircraftReport(**pl) for pl in data]
    return reports_list
