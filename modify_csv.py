import boto3
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import time
from threading import Lock
import uuid

s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'
FAILED_LOG = "/tmp/failed_files.txt"
log_lock = Lock()

def log_failure(file):
    """Preventing threads to try to write at the same times and failing to do so"""
    with log_lock:
        with open(FAILED_LOG, "a") as f:
            f.write(file +"\n")

def read_log_failure():
    """Getting the paths of all the files that failed to be uploaded or downloaded as a list"""
    with open(FAILED_LOG, "r") as f:
        failed_files = [line.strip() for line in f]
    return failed_files

def upload_with_retry(path, file, retry = 5, delay = 2):
    attempt = 0
    while attempt < retry:
        try:
            s3.upload_file(path, bucket, file)
            print(f"{file} successfully uploaded")
            break
        except Exception as e:
            attempt += 1
            if attempt < retry:
                time.sleep(delay)
                print(f"Error while uploading {file}: {e}. Retrying: ({attempt}/{retry}) ...")
            else:
                log_failure(file)
                print(f"Error while uploading {file}: {e}.")

def transform_csv(file, city, location_id, year): 
    long_path = f"/tmp/long_{uuid.uuid4()}.csv"
    wide_path = f"/tmp/wide_{uuid.uuid4()}.csv"
    wide_prefix = f"{city}/wide/{location_id}/{year}/"
    archive_prefix = f"{city}/archive/{location_id}/{year}/"
    file_name = os.path.basename(file)
    wide_file = os.path.join(wide_prefix, file_name)
    archive_file = os.path.join(archive_prefix, file_name)

    try:
        print(f"Processing: {file_name}")
        s3.download_file(bucket, file, long_path)
        df = pd.read_csv(long_path)
        
        if df.empty:
            s3.delete_object(Bucket=bucket, Key=file)
            raise ValueError(f"{file} is empty, skipping it.")
        else:
            df = df.pivot_table(index='datetime', columns='parameter', values='value', aggfunc='mean').reset_index()
            df.to_csv(wide_path, index=False)

            upload_with_retry(wide_path, wide_file)
            upload_with_retry(long_path, archive_file)
            print(f"{file} archived, modified and uploaded to {wide_prefix}")
            s3.delete_object(Bucket=bucket, Key=file)

    except Exception as e:
        print(f"Error with {file}: {e}")
        log_failure(file)

    finally:
        if os.path.exists(long_path): os.remove(long_path)
        if os.path.exists(wide_path): os.remove(wide_path)

def get_years(city, location_id):
    """Get all the list of the years that we have data for this location_id"""
    prefix = f"{city}/{location_id}/"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix, Delimiter='/')
    list_years = [p['Prefix'].split('/')[-2] for p in response.get('CommonPrefixes', [])]
    return list_years

def process_year_folder(city, location_id, year):
    """Processing all the files inside a specific year"""
    prefix = f"{city}/{location_id}/{year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get('Contents', []):
        file = obj['Key']
        if file.endswith('.csv'):
            transform_csv(file, city, location_id, year)

def retry_failed_files():
    """Retry for files that failed during the first batch processing"""
    failed_files_list = read_log_failure()
    still_failed = []
    
    for failed_file in failed_files_list:
        success = False
        failed_file_name_split = failed_file.split('-')
        failed_location_id = failed_file_name_split[1] #Get the location_id of the file
        failed_year = failed_file_name_split[2][0:4] #Get the year of the file
        wide_failed_prefix = f"zurich/wide/{failed_location_id}/{failed_year}/"

        for failure_attempt in range(5):
            try :
                transform_csv(failed_file.strip(), wide_failed_prefix)
                success = True
                break
                
            except Exception as e:
                print(f"Error while uploading {failed_file}: {e}. Retrying: ({failure_attempt}/5) ...")
                time.sleep(20)

        if not success:
            still_failed.append(failed_file)

    with open(FAILED_LOG, "w") as f:
        for file in still_failed:
            f.write(file +"\n")

def main():
    city = "zurich"
    location_ids = [9591, 2453499, 9589, 1236033]
    for location_id in location_ids:
        years = get_years("zurich", location_id)

        with ThreadPoolExecutor(max_workers=4) as executor:
            for year in years:
                executor.submit(process_year_folder, city, location_id, year)
    
    # location_id = 9591  
    # years = ["2022"]   

    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     for year in years:
    #         executor.submit(process_year_folder, city, location_id, year)

    for attempt in range(5):
        try:
            if os.path.getsize(FAILED_LOG) != 0: #If file is not empty, there is some file that weren't processed correctly
                retry_failed_files()
                print("All files have been treated and transformed into wide csv, end of the process.")
            else:
                print("All files have been treated and transformed into wide csv, end of the process.")
                break
        except FileNotFoundError:
            print("All files have been treated and transformed into wide csv, end of the process.")
            break
  
if __name__ == "__main__":
    main()
