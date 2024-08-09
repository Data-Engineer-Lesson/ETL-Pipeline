import boto3 
from config import AWS_ACCESS_KEY,AWS_SECRET_KEY,S3_BUCKET_NAME
import logging

def test_s3_connection():
    try:
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logging.info("Connection success")
    except Exception as e:
        logging.error(f"Failed connection {e}")