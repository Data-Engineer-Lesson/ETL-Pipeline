import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
from config import POSTGRES_USER,POSTGRES_DB,POSTGRES_HOST,POSTGRES_PASSWORD,POSTGRES_PORT

def generate_sample_data(n=100):
    fake=Faker()
    data={
        'name': [fake.name() for _ in range(n)],
        'address': [fake.address() for _ in range(n)],
        'email': [fake.email() for _ in range(n)]
    }
    df = pd.DataFrame(data)
    return df

def extract_data_from_db():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    df = pd.read_sql('SELECT * FROM deneme', engine)
    return df