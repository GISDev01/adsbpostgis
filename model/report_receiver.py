import json
import time

import psycopg2
import requests
import yaml

import logging

from utils import mathutils

logger = logging.getLogger(__name__)

RPTR_FMT = "{:10.10}"
FLT_FMT = "{:8.8}"


class RadioReceiver(object):
    """
    The hardware, such as a Raspberry Pi running PiAware
    """

    name = ""
    type = ""
    long83 = 0.0
    lat83 = 0.0
    data_access_url = ""
    location = ""

    def __init__(self, **kwargs):
        for keyword in ["name", "type", "long83", "lat83", "data_access_url", "location"]:
            setattr(self, keyword, kwargs[keyword])

    def to_JSON(self):
        """Return a string containing a JSON representation of a RadioReceiver"""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, separators=(',', ':'))

    def logToDB(self, db_connection, printQuery=None, update=False):
        """
        Place an instance of a RadioReceiver into the DB - contains name,
        type, location and URL for access
        """

        db_cursor = db_connection.cursor()
        coordinates = "POINT(%s %s)" % (self.lon, self.lat83)
        if update:
            sql = '''
            UPDATE reporter SET (name, type, reporter_location, url) =
            (%s, %s, ST_PointFromText(%s, 4326), %s) where name like %s
            '''
            params = [RPTR_FMT.format(self.name), self.type, coordinates, self.data_access_url,
                      RPTR_FMT.format(self.name)]
        else:
            sql = '''
            INSERT into reporter (name, type, reporter_location, url)
            VALUES (%s, %s, ST_PointFromText(%s, 4326), %s);'''
            params = [RPTR_FMT.format(self.name), self.type, coordinates, self.data_access_url]

        if printQuery:
            print(db_cursor.mogrify(sql, params))
        db_cursor.execute(sql, params)
        db_cursor.close()

    def delFromDB(self, dbconn, printQuery=None):
        """
        Remove an instance of a RadioReceiver from the DB.

        Args:
            dbconn: A psycopg2 DB connection
            printQuery: Triggers printing of constructed query (optional)

        Returns:
            Nothing much

        Raises:
            psycopg2 exceptions
        """
        cur = dbconn.cursor()
        sql = "DELETE from reporter WHERE name like '%s'" % self.name
        if printQuery:
            print(cur.mogrify(sql))
        cur.execute(sql)

    def distance(self, plane):
        """Returns distance in metres from another object with lat/lon"""
        return haversine(self.lon, self.lat83, plane.lon, plane.lat)


def readReporter(dbconn, key="Home1", printQuery=None):
    """
    Read an instance of a RadioReceiver record from the DB.

    Args:
        dbconn: A psycopg2 DB connection
        key: The name of the RadioReceiver record (optional, defaults to Home1)
        printQuery: Triggers printing the SQL query to stdout

    Returns:
        A RadioReceiver object with a location field in WKB format
    """
    from psycopg2.extras import RealDictCursor
    cur = dbconn.cursor(cursor_factory=RealDictCursor)
    sql = '''
		SELECT name, type as mytype, ST_X(reporter_location::geometry) as lon, ST_Y(reporter_location::geometry) as lat, url, reporter_location as location
			FROM reporter WHERE name like \'%s\' ''' % RPTR_FMT.format(key)

    if printQuery:
        print(cur.mogrify(sql))
    cur.execute(sql)
    data = cur.fetchone()
    if data:
        return RadioReceiver(**data)
    else:
        return None

