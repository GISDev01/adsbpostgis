import requests
import io
import zipfile
import os
import datetime

import logging

logger = logging.getLogger(__name__)

current_date_stamp = datetime.datetime.now()


def get_archive_zip(zip_url):
    req = requests.get(zip_url)
    res_zip = zipfile.ZipFile(io.BytesIO(req.content))
    if not os.path.exists('output'):
        os.makedirs('output')
    return res_zip.extractall('output')


get_archive_zip('http://history.adsbexchange.com/Aircraftlist.json/{}.zip'.format(current_date_stamp))
