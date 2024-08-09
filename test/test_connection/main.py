import sys
import os
import logging

# Proje kök dizinini PYTHONPATH'e ekleyin
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .test_s3_connection import test_s3_connection
from .test_postgres_connection import test_postgres_connection
from .test_gcp_connection import test_gcp_connection
from utils.logging_config import setup_logging

def main():
    setup_logging()
    logging.info("Starting connection tests")
    
    logging.info("Testing S3 connection")
    test_s3_connection()
    
    logging.info("Testing PostgreSQL connection")
    test_postgres_connection()

    logging.info("Testing GCP connection")
    test_gcp_connection()
    
    logging.info("All connection tests completed")

if __name__ == '__main__':
    main()