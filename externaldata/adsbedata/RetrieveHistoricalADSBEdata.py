"""
Utilities for downloading historical data in a given AOI.
Python 3.5
"""

import datetime
import io
import logging
import os
import zipfile

import requests
import yaml

from model import aircraft_report

logger = logging.getLogger(__name__)
parent_dir = os.path.dirname(os.path.realpath(__file__))

zip_dir = 'output'
datestamp_format = '%Y-%m-%d'


def get_config():
    with open(os.path.join(parent_dir, 'config.yml'), 'r') as yaml_config_file:
        return yaml.load(yaml_config_file)


def get_and_load_archive_data_by_date(zip_url, zip_filename):
    logger.info('Getting and Loading Archive Data for URL: {}'.format(zip_url))
    extract_dir = zip_filename[:-4]
    if not os.path.exists(os.path.join(zip_dir, extract_dir)):
        req = requests.get(zip_url)
        res_zip = zipfile.ZipFile(io.BytesIO(req.content))
        if not os.path.exists(zip_dir):
            os.makedirs(zip_dir)
        logger.info('Extracting the downloaded zip.')
        res_zip.extractall(os.path.join(zip_dir, extract_dir))
    else:
        logger.warning('Skipping this zip URL - it''s already been downloaded and extracted.')

    aircraft_report.get_aircraft_data_from_files(os.path.join(zip_dir, extract_dir),
                                                 minlat83=minlat83,
                                                 maxlat83=maxlat83,
                                                 minlong83=minlong83,
                                                 maxlong83=maxlong83)


def get_list_of_datestamps_inclusive(start_date, end_date):
    logger.info('Creating list of datestamps to query archival data')
    datestamps_list = []
    start_datestamp = datetime.datetime.strptime(start_date, datestamp_format)
    end_datestamp = datetime.datetime.strptime(end_date, datestamp_format)
    step_size = datetime.timedelta(days=1)
    while start_datestamp <= end_datestamp:
        datestamps_list.append(str(start_datestamp.date()))
        start_datestamp += step_size

    return datestamps_list


local_config = get_config()
archive_base_url = local_config['archive_base_url']
minlat83 = local_config['archiveboundingbox']['minlat83']
maxlat83 = local_config['archiveboundingbox']['maxlat83']
minlong83 = local_config['archiveboundingbox']['minlong83']
maxlong83 = local_config['archiveboundingbox']['maxlong83']

start_date = local_config['startdate']
end_date = local_config['enddate']

start_date = '2018-02-01'
end_date = '2018-02-14'

datestamps_list = get_list_of_datestamps_inclusive(start_date, end_date)

for datestamp in datestamps_list:
    logger.info('Retrieving data for datestamp: {}'.format(datestamp))
    zip_name = '{}.zip'.format(datestamp)
    archive_dl_url = archive_base_url + zip_name
    get_and_load_archive_data_by_date(archive_dl_url, zip_name)
