# Stub out some common entry points to later convert to tests after everything is wired up together

import logging
import time

import yaml

from model import aircraft_report
from model import report_receiver
from utils import postgres as pg_utils

with open("config.yml", 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)

# log_formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")
FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)

# config vars
aircraft_data_url1 = config['feed1']['url']

receiver1_lat83 = config['receiver1']['lat83']
receiver1_long83 = config['receiver1']['long83']

db_hostname = config['database']['hostname']
db_port = config['database']['port']
db_name = config['database']['dbname']
db_user = config['database']['user']
db_pwd = config['database']['pwd']

sleep_time_sec = config['waittimesec']

# TODO: set in config
total_samples_cutoff_val = 1000

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


def crank_it_up():
    logger.debug('Cranking it up.')
    total_samples_count = 0
    while total_samples_count < total_samples_cutoff_val:
        current_time_1 = time.time()

        current_reports_list = aircraft_report.get_aircraft_data_from_url(aircraft_data_url1)
        if len(current_reports_list) > 0:
            aircraft_report.load_aircraft_reports_list_into_db(
                aircraft_reports_list=current_reports_list,
                radio_receiver=radio_receiver_1,
                dbconn=postgres_db_connection)

        current_time_2 = time.time()
        logger.info(str(current_time_2 - current_time_1) + ' seconds for data pull')
        total_samples_count += 1
        time.sleep(sleep_time_sec)


if __name__ == '__main__':
    logger.debug('Entry from main.py main started')
    crank_it_up()
