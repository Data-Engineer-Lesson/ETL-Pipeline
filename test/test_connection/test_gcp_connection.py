import os
from google.cloud import storage
from config import GCP_BUCKET_NAME,GCP_JSON_KEY_PATH
import logging

def test_gcp_connection():
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']=GCP_JSON_KEY_PATH
        client=storage.Client()
        bucket=client.get_bucket(GCP_BUCKET_NAME)
        blobs=list(bucket.list_blobs())
        if blobs:
            logging.info("Connection succesful")
    except Exception as e :
        logging.error(f"Connection failed {e}")