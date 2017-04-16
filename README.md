# adsb-postgis
ADS-B and MLAT data from local raspberry pis ingested through Kafka into Postgres/PostGIS for spatio-temporal analytics

Initial system will ingest straight to Postgres from a single RasPi. After I get that up and running, I will implement 2 more RasPis and a Kafka middleware for better resiliency on data ingestion (without going the easy route of AWS SQS & RDS).