# Stub out some common entry points to later convert to tests after everything is wired up together

import logging
import yaml

from aircraftreport import aircraft

with open("config.yml", 'r') as yaml_config_file:
    config = yaml.load(yaml_config_file)


#log_formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logger = logging.getLogger(__name__)

aircraft_data_url = config['feed']['url']


def crank_it_up():
    logger.debug('Cranking it up.')
    aircraft.get_aircraft_data_from_url(aircraft_data_url)
    pass


if __name__ == '__main__':
    logger.debug('Entry from __main__ started')
    crank_it_up()
    pass
