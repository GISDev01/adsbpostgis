## A quick script to fix the ridiculous format of the raw FAA download data (at least it's available though!)

import csv

# MASTER.txt is from https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download/
with open("MASTER.txt") as orig_file:
    orig_file_reader = csv.reader(orig_file, delimiter=",")
    with open("/temp/MASTER_CLEANED.csv", "w", newline='') as clean_file:
        writer = csv.writer(clean_file)
        for orig_record in orig_file_reader:
            new_row = [old_field_data.strip() for old_field_data in orig_record]
            # The data has a trailing comma on every single row (including the header row), so remove it as well
            new_row_without_trailing_comma = new_row[:-1]
            writer.writerow(new_row_without_trailing_comma)
