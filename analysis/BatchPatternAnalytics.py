import logging
import time

import yaml

from utils import postgres as pg_utils

with open('../config.yml', 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)

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


def get_all_unique_itinerary_ids_without_patterns_analyzed():
    """
    Queries the database to find all of the unqiue mode_s_hex codes that have at least 1 record without an itinerary ID
    assigned (null)

    :return: list of Mode S Hex IDs (strings) that have at least 1 record without an itinerary ID assigned
    """
    logger.info('Fetching a list of all Mode-s hex codes that are missing at least 1 itinerary ID.')
    uniq_mode_s_cursor = dbconn.cursor()

    sql = '''SELECT 
              DISTINCT aircraftreports.mode_s_hex 
                FROM aircraftreports 
                  WHERE aircraftreports.itinerary_id IS NULL'''
    uniq_mode_s_cursor.execute(sql)

    return [record[0] for record in uniq_mode_s_cursor.fetchall()]


def assign_itinerary_id_for_mode_s(mode_s_hex_for_update, itinerary_id, min_time, max_time):
    """
    Given a mode s hex code, itinerary id, and 2 epoch timestamps, assign the itinerary ID to the appropriate rows

    :param mode_s_hex_for_update: Mode-s hex code (str)
    :param itinerary_id: itinerary ID (str)
    :param min_time: epoch timestamp, minimum timestamp
    :param max_time: epoch timestamp, maximum timestamp

    """
    min_timestamp = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(min_time))
    max_timestamp = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(max_time))

    logger.info(
        'Assigning Itinerary ID {} for Mode S {} between times {} and {}'.format(itinerary_id,
                                                                           mode_s_hex_for_update,
                                                                           min_timestamp,
                                                                           max_timestamp))
    logger.debug(
        'Between {} and {}'.format(min_time, max_time))

    itinerary_cursor = dbconn.cursor()

    sql = "UPDATE aircraftreports SET itinerary_id = '{0}' WHERE aircraftreports.mode_s_hex = '{1}' " \
          "AND aircraftreports.report_epoch BETWEEN {2} AND {3} ".format(itinerary_id,
                                                                         mode_s_hex_for_update,
                                                                         min_time,
                                                                         max_time)
    logger.debug('Assigning Itinerary ID with sql: {}'.format(sql))

    itinerary_cursor.execute(sql)

    # commit the query for each of the itinerary assignments as we loop through them
    dbconn.commit()
    itinerary_cursor.close()



itinerary_id_list_to_process = get_all_unique_mode_s_without_itin_assigned()

num_to_process = len(mode_s_list_to_process)

mode_s_count = 0

for mode_s in mode_s_list_to_process:
    mode_s_count += 1
    logger.info('Calcing Itinerary IDs for Mode S: {} - Progress: {}/{}'.format(mode_s, mode_s_count, num_to_process))
