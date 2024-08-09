import sys
import os 
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from test.test_connection.test_postgres_connection import test_postgres_connection


def main():
    test_postgres_connection()
    test_s3_connection()
    test_gcp_connection()


if __name__=='__main__':
    main()
