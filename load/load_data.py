from sqlalchemy import create_engine
import pandas as pd
import boto3
import os
from google.cloud import storage
from io import BytesIO
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_DB, POSTGRES_HOST
import csv

def load_data_to_postgres(df):
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    df.to_sql('deneme', engine, if_exists='replace', index=False)

def save_dataframe_to_excel(df,file_name):
    with BytesIO() as buffer:
        with pd.ExcelWriter(buffer,engine='openpyxl') as writer:
            df.to_excel(writer,index=False,sheet_name='Sheet1')
        buffer.seek(0)
        return buffer.read()

def save_dataframe_to_csv(df,filename,bucket_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']="C:/Users/Ceren/Desktop/ETL-Pipeline/optical-metric-387612-c8a90b8cb73e.json"

    csv_data = df.to_csv(index=False,quoting=csv.QUOTE_ALL)


    client=storage.Client()
    bucket=client.get_bucket(bucket_name)
    blob=bucket.blob(filename)
    blob.upload_from_string(csv_data, content_type='text/csv')

def upload_to_s3(data,bucket_name,file_name):
     s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
     s3_client.put_object(Bucket=bucket_name,Key=file_name,Body=data)
     print("Upload success")

def upload_to_gcp(data,bucket_name,file_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']="C:/Users/Ceren/Desktop/ETL-Pipeline/optical-metric-387612-c8a90b8cb73e.json"
    client=storage.Client()
    bucket=client.get_bucket(bucket_name)
    blob=bucket.blob(file_name)
    blob.upload_from_string(data, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    print(f'File {file_name} uploaded to GCP bucket {bucket_name}')
