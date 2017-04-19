"""
Postgres DB Utilities
"""

import psycopg2
import logging
logger = logging.getLogger(__name__)


def database_connection(dbuser=None, dbhost=None, dbpasswd=None, dbport=5432):
    """
    Makes a connection to a Postgres DB

    Args:
        dbuser: Contains name of database user to login (optional)
        dbhost: Name of host that DB is running on (optional)
        dbpasswd: Password for the DB account (optional)
        dbport: Portnumber to connect to on DB host (optional)

    Returns:
        psycopg2 DB connection

    """
    connection = None

    # TODO: Pull dbname out into param/argument
    connect_str = "dbname=AircraftReports user=" + dbuser + " host=" + \
                  dbhost + " password=" + dbpasswd + " port=" + str(dbport)
    try:
        connection = psycopg2.connect(connect_str)
    except:
        print("Can't connect to aircraft report database with " + connect_str)
        exit(-1)
    return connection

