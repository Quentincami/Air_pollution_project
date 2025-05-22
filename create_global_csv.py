import boto3
import pandas as pd
import uuid
import os
import time
from datetime import date


s3 = boto3.client('s3')
bucket = 'openaq-sensor-data'

def combine_yearly_files(retry = 5, delay = 2):
    city = "lyon"
    prefix = f"{city}/wide/yearly_files"
    archive_prefix = f"{city}/archive/yearly_files/"
    temp_path = f"/tmp/file_{uuid.uuid4()}.csv"

    today = date.today()
    date_str = today.strftime("%Y-%m-%d") 

    output_key = f"{city}/wide/global_files/global_{city}_{date_str}_file.csv"

    attempt = 0
    while attempt < retry:
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

            dfs = []
            for file_key in files:
                try:
                    obj = s3.get_object(Bucket=bucket, Key=file_key)
                    df=pd.read_csv(obj['Body'])
                    print(f"Processing {file_key} ...")
                    dfs.append(df)

                    file_name = os.path.basename(file_key)
                    archived_key = f"{archive_prefix}/{file_name}"
                    s3.copy_object(Bucket=bucket, CopySource ={'Bucket': bucket, 'Key': file_key}, Key=archived_key)


                    s3.delete_object(Bucket=bucket, Key=file_key)

                except Exception as e:
                    print(f"Failed to process {file_key}: {e}")

            if not dfs:
                print(f"No files to merge.")
                return    

            print("Concatening all files into one")
            try:
                df_all = pd.concat(dfs, ignore_index=True)
                df_avg = df_all.groupby("datetime", as_index = False).mean()
                df_avg.to_csv(temp_path, index = False)

                s3.upload_file(temp_path, bucket, output_key)
                print(f"{output_key} created and saved.")
                break
            except Exception as e:
                print(f"Failed to create global file :{e}")

        except Exception as e:
            attempt += 1
            if attempt < retry:
                time.sleep(delay)
                print(f"Error while creating file: {e}. Retrying: ({attempt}/{retry}) ...")
            else:
                print(f"File not created : {e}.")

    if os.path.exists(temp_path):
        os.remove(temp_path)

def main():
    combine_yearly_files()
    
if __name__ == "__main__":
  main()