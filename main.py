# Stub out some common entry points to later convert to tests after everything is wired up together

import logging
import time
import os
import yaml

from model import aircraft_report
from model import report_receiver
from utils import postgres as pg_utils

with open("config.yml", 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)

# log_formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")
FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

# config vars
aircraft_data_url1 = config['feed1']['url']
receiver1_lat83 = config['receiver1']['lat83']
receiver1_long83 = config['receiver1']['long83']

aircraft_data_url2 = config['feed2']['url']
receiver2_lat83 = config['receiver2']['lat83']
receiver2_long83 = config['receiver2']['long83']

db_hostname = config['database']['hostname']
db_port = config['database']['port']
db_name = config['database']['dbname']
db_user = config['database']['user']
db_pwd = config['database']['pwd']

sleep_time_sec = config['waittimesec']

total_samples_cutoff_val = config['samplescutoff']

postgres_db_connection = pg_utils.database_connection(dbname=db_name,
                                                      dbhost=db_hostname,
                                                      dbport=db_port,
                                                      dbuser=db_user,
                                                      dbpasswd=db_pwd)

radio_receiver_1 = report_receiver.RadioReceiver(name='piaware1',
                                                 type='raspi',
                                                 lat83=receiver1_lat83,
                                                 long83=receiver1_long83,
                                                 data_access_url='',
                                                 location="")

radio_receiver_2 = report_receiver.RadioReceiver(name='piaware2',
                                                 type='raspi',
                                                 lat83=receiver2_lat83,
                                                 long83=receiver2_long83,
                                                 data_access_url='',
                                                 location="")


def harvest_aircraft_json_from_pi():
    logger.info('Aircraft ingest beginning.')
    total_samples_count = 0
    failure_num = 0
    while total_samples_count < total_samples_cutoff_val:
        try:
            start_time = time.time()

            current_reports_list = aircraft_report.get_aircraft_data_from_url(aircraft_data_url1)
            if len(current_reports_list) > 0:
                aircraft_report.load_aircraft_reports_list_into_db(
                    aircraft_reports_list=current_reports_list,
                    radio_receiver=radio_receiver_1,
                    dbconn=postgres_db_connection)

            end_time = time.time()
            logger.debug('{} seconds for data pull from Pi'.format((end_time - start_time)))
            total_samples_count += 1
            time.sleep(sleep_time_sec)
        except:
            # Workaround for failing connection when pi gets busy
            logger.exception('Issue getting data from a receiver {}'.format(radio_receiver_1))
            time.sleep(120)
            failure_num += 1
            if failure_num > 10:
                exit(1)



if __name__ == '__main__':
    logger.debug('Entry from main.py main started')
    harvest_aircraft_json_from_pi()
    # directory_path = os.path.join('externaldata', 'adsbedata', 'rawdata', '2016-06-20')
    # aircraft_report.get_aircraft_data_from_files(directory_path)
