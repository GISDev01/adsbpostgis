import logging
import time

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

ITINERARY_MAX_TIME_DIFF_SECONDS = int(config['itinerarymaxtimediffseconds'])

dbconn = pg_utils.database_connection(dbname=db_name,
                                      dbhost=db_hostname,
                                      dbport=db_port,
                                      dbuser=db_user,
                                      dbpasswd=db_pwd)


def get_all_unique_mode_s_without_itin_assigned():
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


def calc_time_diffs_for_mode_s(mode_s_hex):
    """
    Given an input of a string mode_s_hex code, query the DB for all records with that mode_s_hex and loop through
    the record in order of timestamp, comparing each pair of records to determine the amount of time between
    each point. If the 2 points are far arapt, it is assumed that the aircraft landed and took back off.
    Note: this doesn't assign an ID for all records (the most recent batch), because the DB could be in the
    middle of an itinerary when this script is run.

    :param mode_s_hex: the hex code identifying the aircraft
    :type mode_s_hex: str

    """

    logger.info('Calcing Time Diffs to assign itinerary ids for mode s: {}'.format(mode_s_hex))
    uniq_mode_s_cursor = dbconn.cursor()

    sql = '''SELECT aircraftreports.report_epoch, aircraftreports.report_epoch - lag(aircraftreports.report_epoch)
                OVER (ORDER BY aircraftreports.report_epoch) 
                  AS time_delta_sec
             FROM aircraftreports 
              WHERE aircraftreports.itinerary_id IS NULL AND aircraftreports.mode_s_hex = '{}'
                    ORDER BY aircraftreports.report_epoch'''.format(mode_s_hex)

    uniq_mode_s_cursor.execute(sql)

    count = 0
    for time_diff_tuple in uniq_mode_s_cursor.fetchall():
        curr_timestamp = time_diff_tuple[0]

        # Time difference between the current record and the previous record
        time_diff_sec = time_diff_tuple[1]

        if count == 0:
            minimum_timestamp = time_diff_tuple[0]
            count += 1
            continue

        if time_diff_sec > ITINERARY_MAX_TIME_DIFF_SECONDS:
            maximum_timestamp = curr_timestamp
            assign_itinerary_id_for_mode_s(itinerary_id=generate_itinerary_id(mode_s_hex, minimum_timestamp),
                                           mode_s_hex_for_update=mode_s_hex,
                                           min_time=minimum_timestamp,
                                           max_time=maximum_timestamp)
            count = 0
        else:
            count += 1


def generate_itinerary_id(mode_s, epoch_timestamp):
    """
    Using the mode-s hex code and the minimum epoch timestamp, create a new string that will be used as the
    unique itinerary ID

    :param mode_s: mode-s hex code
    :type mode_s: str
    :param epoch_timestamp: minimum timestamp that starts the itinerary
    :type epoch_timestamp: int
    :return: the itinerary ID (str)

    """

    timestamp = time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(epoch_timestamp))
    itinerary_id_generated = timestamp + '_{}'.format(mode_s)

    return itinerary_id_generated


mode_s_list_to_process = get_all_unique_mode_s_without_itin_assigned()

num_to_process = len(mode_s_list_to_process)

mode_s_count = 0

for mode_s in mode_s_list_to_process:
    mode_s_count += 1
    logger.info('Calcing Itinerary IDs for Mode S: {} - Progress: {}/{}'.format(mode_s, mode_s_count, num_to_process))
    calc_time_diffs_for_mode_s(mode_s)
