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

ITINERARY_MAX_TIME_DIFF_SECONDS = config['itinerarymaxtimediffseconds']

dbconn = pg_utils.database_connection(dbname=db_name,
                                      dbhost=db_hostname,
                                      dbport=db_port,
                                      dbuser=db_user,
                                      dbpasswd=db_pwd)


def get_all_unique_mode_s_without_itin_assigned():
    logger.info('Fetching a list of all Mode S Idents missing itin ID.')
    uniq_mode_s_cursor = dbconn.cursor()

    sql = '''SELECT DISTINCT aircraftreports.mode_s_hex FROM aircraftreports 
              WHERE aircraftreports.itinerary_id IS NULL'''
    uniq_mode_s_cursor.execute(sql)

    return [record[0] for record in uniq_mode_s_cursor.fetchall()]


def assign_itinerary_id_for_mode_s(mode_s_hex_for_update, itinerary_id, min_time, max_time):
    logger.info(
        'Assigning Itin ID {} for Mode S {} between {} and {}'.format(itinerary_id,
                                                                      mode_s_hex_for_update,
                                                                      min_time,
                                                                      max_time))

    itinerary_cursor = dbconn.cursor()

    itinerary_cursor.execute("UPDATE aircraftreports "
                             "SET aircraftreports.itinerary_id='{0}' "
                             "WHERE aircraftreports.mode_s_hex = '{1}' "
                             "AND aircraftreports.report_epoch BETWEEN {2} AND {3} ".format(itinerary_id,
                                                                                            mode_s_hex_for_update,
                                                                                            min_time,
                                                                                            max_time))


def calc_time_diffs_for_mode_s(mode_s_hex):
    logger.info('Calcing Time Diffs to assign itinery ids for mode s: {}'.format(mode_s_hex))
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
        logger.debug('Time Diff: {}'.format(time_diff_tuple))
        curr_timestamp = time_diff_tuple[0]

        # Time difference between the current record and the previous record
        time_diff_sec = time_diff_tuple[1]

        if count == 0:
            minimum_timestamp = time_diff_tuple[0]

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
    timestamp = time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(epoch_timestamp / 1000.))
    itineraryid = timestamp + '_{}'.format(mode_s)
    logger.info('Itinerary ID Generated: {}'.format(itineraryid))
    return itineraryid


# get_unique_mode_s_without_itin_assigned()
calc_time_diffs_for_mode_s('ADAFB5')
