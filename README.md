# adsb-postgis
ADS-B and MLAT data from local raspberry pis ingested through Kafka into Postgres/PostGIS for spatio-temporal analytics

Initial system will ingest straight to Postgres from a single RasPi running the latest PiAware distro on the LAN.
The next phase will implement 2 more RasPis (on the WAN, not LAN) and a Kafka middleware for better resiliency
with data ingestion (without going the easy route of AWS SQS & RDS due to cost).

Tested on Python 3.5 and 3.6 with Anaconda distro. (all requirements are in the requirements.txt) on both
Windows 10 on Thinkpad T570 and MacOS Sierra on MBPt. Testing on Postgres 9.6 x64 with PostGIS 2.3.2.

conda create -n adsbpostgis python=3.6
On Mac: brew install postgis

git clone https://github.com/GISDev01/adsbpostgis.git
cd adsbpostgis
Mac/Ubuntu: source activate adsbpostgis
Windows:    activate adsbpostgis
pip install -r requirements.txt

Then, edit the config.yml.template to match you environment, and save as: config.yml

Feel free to open a GitHub issue or email me if you have any issues getting this project up and running locally.