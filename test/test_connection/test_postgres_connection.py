import psycopg2
from config import POSTGRES_USER,POSTGRES_DB,POSTGRES_HOST,POSTGRES_PASSWORD,POSTGRES_PORT
import logging

def test_postgres_connection():
    try:
        connection = psycopg2.connect(
            user = POSTGRES_USER,
            password = POSTGRES_PASSWORD,
            database = POSTGRES_DB,
            host =POSTGRES_HOST,
            port = POSTGRES_PORT
        )
        cursor = connection.cursor()
        cursor.execute("select version();")
        record = cursor.fetchone()
        logging.info(f"Connected to Postgre.Version: {record}")
    except Exception as e:
        logging.error(f"Failed {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

