import logging

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


def get_unique_mode_s_without_itin_assigned():
    uniq_mode_s_cursor = dbconn.cursor()

    sql = '''
    		SELECT DISTINCT aircraftreports.mode_s_hex
    			FROM aircraftreports 
    			    WHERE aircraftreports.itinerary_id IS NULL'''
    uniq_mode_s_cursor.execute(sql)

    return [item[0] for item in uniq_mode_s_cursor.fetchall()]


def assign_itinerary_id_for_mode_s(mode_s_hex_for_update, itinerary_id, min_time, max_time):
    itinerary_cursor = dbconn.cursor()

    itinerary_cursor.execute("UPDATE aircraftreports "
                             "SET aircraftreports.itinerary_id='{0}' "
                             "WHERE aircraftreports.mode_s_hex = '{1}' "
                             "AND aircraftreports.report_epoch BETWEEN {2} AND {3} ".format(itinerary_id,
                                                                                            mode_s_hex_for_update,
                                                                                            min_time,
                                                                                            max_time))


def get_records_to_assign_new_itinerary_id(mode_s):
    all_timestamps_per_mode_s_hex = [item for item in cur.fetchall()]

    i = 0
    num_rows = len(all_timestamps_per_mode_s_hex)
    for row in all_timestamps_per_mode_s_hex:
        timestamp1 = row[13]
        timestamp2 = all_timestamps_per_mode_s_hex[i + 1][13]
        time_diff_in_secs = timestamp2 - timestamp1
        logger.info('{}'.format(time_diff_in_secs))
        i += 1
        # Skip the last record in the list
        if i == num_rows - 1:
            break


def calc_time_diff_for_mode_s(mode_s_hex):
    uniq_mode_s_cursor = dbconn.cursor()

    sql = '''SELECT aircraftreports.report_epoch, aircraftreports.report_epoch - lag(aircraftreports.report_epoch)
                OVER (ORDER BY aircraftreports.report_epoch) 
                  AS time_delta_sec
             FROM aircraftreports 
              WHERE aircraftreports.itinerary_id IS NULL AND aircraftreports.mode_s_hex = '{}'
                    ORDER BY aircraftreports.report_epoch'''.format(mode_s_hex)

    uniq_mode_s_cursor.execute(sql)

    for time_diff_tuple in uniq_mode_s_cursor.fetchall():
        logger.info(time_diff_tuple)


# get_unique_mode_s_without_itin_assigned()
calc_time_diff_for_mode_s('ADAFB5')
