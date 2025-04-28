import boto3
import gzip
import os

s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'

def unzip_and_upload(key):
    gz_path = '/tmp/temp.gz'
    csv_path = '/tmp/temp.csv'

    try:
        print(f"Processing: {key}")
        s3.download_file(bucket, key, gz_path)

        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                f_out.write(f_in.read())

        new_key = key.replace('.csv.gz', '.csv')
        s3.upload_file(csv_path, bucket, new_key)
        print(f"✅ Uploaded: {new_key}")

        # Delete original .gz
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"🗑️ Deleted: {key}")

    except Exception as e:
        print(f"❌ Error with {key}: {e}")
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

process_year_folder('zurich', '9591', '2020')
