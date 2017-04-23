# Stub out some common entry points to later convert to tests after everything is wired up together

import logging

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

postgres_db_connection = pg_utils.database_connection(dbhost=db_hostname, dbport=db_port, dbuser=db_user,
                                                      dbpasswd=db_pwd)

radio_receiver_1 = report_receiver.RadioReceiver(name='bodge', type='piaware1', lat83=receiver1_lat83,
                                                 long83=receiver1_long83,
                                                 data_access_url='', location="")


def crank_it_up():
    logger.debug('Cranking it up.')
    current_reports_list = aircraft_report.get_aircraft_data_from_url(aircraft_data_url1)
    aircraft_report.load_aircraft_reports_list_into_db(aircraft_reports_list=current_reports_list,
                                                       radio_receiver=radio_receiver_1,
                                                       dbconn=postgres_db_connection)


if __name__ == '__main__':
    logger.debug('Entry from __main__ started')
    crank_it_up()
