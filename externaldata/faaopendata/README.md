For extra functionality, we can load the FAA Open Data database into Postgres for fast querying and filtering.

Find the CSV download here:
https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download/

Metadata can be found in this pdf:
https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/media/ardata.pdf

Then, we need to clean the downloaded MASTER.txt with the included faa_data_cleaner.py data pre-processor.

This removes the trailing comma (the last character) of every single line in the 150MB text file. This will take about 10-20 seconds or so to run on a modern machine.

And load it into your local Postgres DB with the .sql script in this folder: faadbimport.sql

If the data is missing, we can try an ajax lookup here: http://registry.faa.gov/aircraftinquiry/NNum_Results.aspx?nNumberTxt=NNumHere later. There are also a few other places we can query as well, if we need to.