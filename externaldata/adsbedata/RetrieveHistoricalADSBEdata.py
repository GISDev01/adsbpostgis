"""
Utilities for downloading historical data in a given AOI.
Python 3.5
"""

import requests
import io
import zipfile
import os
import datetime
import logging
import yaml
from model import aircraft_report

logger = logging.getLogger(__name__)
zip_dir = 'output'

# TODO: dynamically calculate range of dates
# current_date_stamp = strftime('%y-%m-%d')
current_date_stamp = '2017-10-01'
datestamp_format = '%Y-%m-%d'
parent_dir = os.path.dirname(os.path.realpath(__file__))


def get_config():
    with open(os.path.join(parent_dir, 'config.yml'), 'r') as yaml_config_file:
        return yaml.load(yaml_config_file)


def get_and_load_archive_data_by_date(zip_url):
    # req = requests.get(zip_url)
    # res_zip = zipfile.ZipFile(io.BytesIO(req.content))
    # if not os.path.exists(zip_dir):
    #     os.makedirs(zip_dir)
    # res_zip.extractall(zip_dir)

    aircraft_report.get_aircraft_data_from_files(os.path.join(parent_dir, zip_dir))


def get_list_of_datestamps_inclusive(start_date, end_date):
    datestamps_list = []
    start_datestamp = datetime.datetime.strptime(start_date, datestamp_format)
    end_datestamp = datetime.datetime.strptime(end_date, datestamp_format)
    step_size = datetime.timedelta(days=1)
    while start_datestamp <= end_datestamp:
        datestamps_list += start_datestamp
        start_datestamp += step_size

    return datestamps_list


local_config = get_config()
adsbe_download_base_url = local_config['adsbe_url']

dl_url = adsbe_download_base_url + '{}.zip'.format(current_date_stamp)
print(dl_url)
get_and_load_archive_data_by_date(dl_url)
