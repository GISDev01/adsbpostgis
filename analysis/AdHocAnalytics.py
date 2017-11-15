import logging
import time
import ppygis3

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


def placeholder(mode_s_hex):
    pattern_cursor = dbconn.cursor()

    sql = "SELECT * FROM find_pattern_num('{}');".format(mode_s_hex)

    pattern_cursor.execute(sql)

    for record in pattern_cursor.fetchall():
        print(ppygis3.Geometry.read_ewkb(record[3]))