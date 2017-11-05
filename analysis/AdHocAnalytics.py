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

cur = dbconn.cursor()
sql = '''
		SELECT *
			FROM aircraftreports 
			    WHERE aircraftreports.mode_s_hex LIKE 'ADAFB5'
			        ORDER BY report_epoch '''

cur.execute(sql)
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
    if i == num_rows-1:
        break

