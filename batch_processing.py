import boto3
import gzip
import os
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'

def unzip_and_upload(file):
    gz_path = '/tmp/temp.gz'
    csv_path = '/tmp/temp.csv'

    try:
        print(f"Processing: {file}")
        s3.download_file(bucket, file, gz_path)

        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                f_out.write(f_in.read())

        new_file = file.replace('.csv.gz', '.csv')
        s3.upload_file(csv_path, bucket, new_file)
        print(f"Uploaded: {new_file}")

        # Delete original .gz
        s3.delete_object(Bucket=bucket, Key=file)
        print(f"Deleted: {file}")

    except Exception as e:
        print(f"Error with {file}: {e}")
    finally:
        if os.path.exists(gz_path): os.remove(gz_path)
        if os.path.exists(csv_path): os.remove(csv_path)

def process_year_folder(city, location_id, year):
    prefix = f"{city}/{location_id}/{year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv.gz'):
            unzip_and_upload(key)

def get_years(city, location_id):
    prefix = f"{city}/{location_id}/"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix, Delimiter='/')
    list_years = [p['Prefix'].split('/')[-2] for p in response.get('CommonPrefixes', [])]
    return list_years

def main():
    city = "zurich"
    location_ids = [9591, 2453499, 9589, 1236033]
    for location_id in location_ids:
        years = get_years("zurich", location_id)

        with ThreadPoolExecutor(max_workers=4) as executor:
            for year in years:
                executor.submit(process_year_folder, city, location_id, year)


if __name__ == "__main__":
    main()
