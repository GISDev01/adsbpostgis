import logging
import time
import ppygis3

import yaml

from utils import postgres as pg_utils

with open('../config.yml', 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)

# log_formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")
FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

db_hostname = config['database']['hostname']
db_port = config['database']['port']
db_name = config['database']['dbname']
db_user = config['database']['user']
db_pwd = config['database']['pwd']


dbconn = pg_utils.database_connection(dbname=db_name,
                                      dbhost=db_hostname,
                                      dbport=db_port,
                                      dbuser=db_user,
                                      dbpasswd=db_pwd)


def placeholder(mode_s_hex_for_update, itinerary_id, min_time, max_time):
    pass
    # itinerary_cursor = dbconn.cursor()
    #
    # itinerary_cursor.execute("UPDATE aircraftreports "
    #                          "SET aircraftreports.itinerary_id='{0}' "
    #                          "WHERE aircraftreports.mode_s_hex = '{1}' "
    #                          "AND aircraftreports.report_epoch BETWEEN {2} AND {3} ".format(itinerary_id,
    #                                                                                         mode_s_hex_for_update,
    #                                                                                         min_time,
    #                                                                                         max_time))


def find_patterns_within_itinerary(mode_s_hex):
    pattern_cursor = dbconn.cursor()

    sql = "SELECT * FROM find_pattern_num('A8D33A');".format(mode_s_hex)

    pattern_cursor.execute(sql)

    # patternnumber    INT,
    # patterngeometry GEOMETRY,
    # patternstartend GEOMETRY,
    # patterncentroid GEOMETRY

    for record in pattern_cursor.fetchall():
        # Get coords of the centroid
        print(ppygis3.Geometry.read_ewkb(record[3]))

        #print(record)
        #logger.info('Pattern Result: {}'.format(ppygis3.Point(record[3])))
        #for field in record:

        print(ppygis3.Geometry.read_ewkb(record[3]))
        # logger.info('Pattern Result: {}'.format(LineString(record[2])))


find_patterns_within_itinerary('')