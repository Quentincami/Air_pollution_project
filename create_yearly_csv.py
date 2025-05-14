import boto3
import pandas as pd
import uuid
import os
from concurrent.futures import ThreadPoolExecutor
import time


s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'

def get_years(city, location_id):
    """Get all the list of the years that we have data for this location_id"""
    prefix = f"{city}/wide/{location_id}/"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix, Delimiter='/')
    list_years = [p['Prefix'].split('/')[-2] for p in response.get('CommonPrefixes', [])]
    return list_years

def combine_yearly_files(city, location_id, year, retry = 5, delay = 2):
    prefix = f"{city}/wide/{location_id}/{year}/"
    temp_path = f"/tmp/file_{uuid.uuid4()}.csv"
    output_key = f"{city}/wide/yearly_files/{location_id}_{year}.csv"

    attempt = 0
    while attempt < retry:
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

            dfs = []
            for file_key in files:
                obj = s3.get_object(Bucket=bucket, Key=file_key)
                df=pd.read_csv(obj['Body'])
                dfs.append(df)

            if not dfs:
                print(f"No data for {location_id} in {year}.")
                return    

            df_all = pd.concat(dfs, ignore_index=True)
            df_avg = df_all.groupby("datetime", as_index = False).mean()
            df_avg.to_csv(temp_path, index = False)

            s3.upload_file(temp_path, bucket, output_key)
            print(f"{output_key} created and saved.")
            break

        except Exception as e:
            attempt += 1
            if attempt < retry:
                time.sleep(delay)
                print(f"Error while creating file for {location_id} and {year}: {e}. Retrying: ({attempt}/{retry}) ...")
            else:
                print(f"File for {location_id} and {year} not created : {e}.")

    if os.path.exists(temp_path):
        os.remove(temp_path)

def main():
    city = "lyon"
    location_ids = [3647, 2696, 3638, 3586]

    for location_id in location_ids:
            years = get_years(city, location_id)

            with ThreadPoolExecutor(max_workers=4) as executor:
                for year in years:
                    executor.submit(combine_yearly_files, city, location_id, year)
    
if __name__ == "__main__":
  main()