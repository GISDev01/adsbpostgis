import requests
import io
import zipfile

import logging

logger = logging.getLogger(__name__)


def get_archive_zip(zip_url):
    req = requests.get(zip_url)
    res_zip = zipfile.ZipFile(io.BytesIO(req.content))

    return res_zip.extractall()


get_archive_zip('http://history.adsbexchange.com/Aircraftlist.json/2016-06-20.zip')
