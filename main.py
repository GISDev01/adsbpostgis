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
aircraft_data_url = config['feed']['url']

receiver_lat83 = config['receiver']['lat83']
receiver_long83 = config['receiver']['long83']

db_hostname = config['database']['hostname']
db_port = config['database']['port']
db_name = config['database']['dbname']
db_user = config['database']['user']
db_pwd = config['database']['pwd']

postgres_db_connection = pg_utils.database_connection(dbhost=db_hostname, dbport=db_port, dbuser=db_user,
                                                      dbpasswd=db_pwd)
#        for keyword in ["name", "type", "long83", "lat83", "data_access_url", "location"]:

radio_receiver = report_receiver.RadioReceiver(name='bodge', type='', lat83=receiver_lat83, long83=receiver_long83,
                                               data_access_url='', location="")


def crank_it_up():
    logger.debug('Cranking it up.')
    aircraft_report.get_aircraft_data_from_url(aircraft_data_url)
    pass


if __name__ == '__main__':
    logger.debug('Entry from __main__ started')
    crank_it_up()
    pass
