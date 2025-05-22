import boto3
import os
import uuid
import psycopg2
from urllib.parse import urlparse

s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'
prefix = "lyon/wide/global_files/"
archive_prefix = "lyon/archive/global_files/"

# Connect to PostgreSQL
timescale_url = os.getenv("TIMESCALE_SERVICE_URL")

def upload_csv_to_db(file_key, conn):
    temp_path = f"/tmp/file_{uuid.uuid4()}.csv"
    try:
        s3.download_file(bucket, file_key, temp_path)

        with conn.cursor() as cursor, open(temp_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(
                sql="COPY air_quality FROM STDIN WITH CSV HEADER",
                file=f
            )
            conn.commit()
            print(f"{file_key} uploaded successfully.")

        file_name = os.path.basename(file_key)
        archived_key = f"{archive_prefix}/{file_name}"
        s3.copy_object(Bucket=bucket, CopySource ={'Bucket': bucket, 'Key': file_key}, Key=archived_key)

        s3.delete_object(Bucket=bucket, Key=file_key)

    except Exception as e:
        print(f"Error with {file_key}: {e}")

    finally:
      if os.path.exists(temp_path):
          os.remove(temp_path)

def main():
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    content = response.get('Contents', [])

    if not content:
        print("No files to upload")
        return
    
    global_csv = [obj['Key'] for obj in content if obj['Key'].endswith('.csv')][0]

    url = urlparse(timescale_url)

    # Connect to database
    with psycopg2.connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path[1:],
        user=url.username,
        password=url.password
    ) as conn:
        upload_csv_to_db(global_csv, conn)

if __name__ == "__main__":
  main()