import boto3
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import time
from threading import Lock
import uuid
import psycopg2
from urllib.parse import urlparse

s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'
FAILED_LOG = "/tmp/failed_files.txt"
log_lock = Lock()

# Connect to PostgreSQL
timescale_url = os.getenv("TIMESCALE_SERVICE_URL")

def log_failure(file, error):
    with log_lock:
        with open(FAILED_LOG, "a") as f:
            f.write(f"{file} - {error}\n")

def upload_csv_to_db(file, conn):
    temp_path = f"/tmp/file_{uuid.uuid4()}.csv"
    try:
        s3.download_file(bucket, file, temp_path)


        with conn.cursor() as cursor, open(temp_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(
                sql="COPY air_quality FROM STDIN WITH CSV HEADER",
                file=f
            )
            conn.commit()
            print(f"{file} uploaded successfully.")

    except Exception as e:
        print(f"Error with {file}: e")
        log_failure(file, e)

    finally:
      if os.path.exists(temp_path):
          os.remove(temp_path)

def get_years(city, location_id):
    """Get all the list of the years that we have data for this location_id"""
    prefix = f"{city}/archive/{location_id}/"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix, Delimiter='/')
    list_years = [p['Prefix'].split('/')[-2] for p in response.get('CommonPrefixes', [])]
    return list_years

def process_year_folder(city, location_id, year, conn):
    """Processing all the files inside a specific year"""
    prefix = f"{city}/archive/{location_id}/{year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get('Contents', []):
        file = obj['Key']
        if file.endswith('.csv'):
            upload_csv_to_db(file, conn)

def main():
    city = "zurich"
    location_ids = [9591, 2453499, 9589, 1236033]
    url = urlparse(timescale_url)

    # Connect to database
    with psycopg2.connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path[1:],
        user=url.username,
        password=url.password
    ) as conn:

        for location_id in location_ids:
            years = get_years(city, location_id)

            with ThreadPoolExecutor(max_workers=4) as executor:
                for year in years:
                    executor.submit(process_year_folder, city, location_id, year, conn)

if __name__ == "__main__":
  main()