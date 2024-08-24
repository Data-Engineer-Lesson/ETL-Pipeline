import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
from config import POSTGRES_USER,POSTGRES_DB,POSTGRES_HOST,POSTGRES_PASSWORD,POSTGRES_PORT
import random

def load_sample_data_to_db():
    fake=Faker()

    def generate_customers(n=100):
        data={
            "customer_id":[fake.uuid4() for _ in range(n)],
            "name":[fake.name() for _ in range (n)],
            "email":[fake.email() for _ in range(n)],
            "address":[fake.address() for _ in range(n)],
            "registration_date":[fake.date_this_decade() for _ in range(n)]
        }
        return pd.DataFrame(data)

    def generate_orders(customers,n=200):
        data={
            "order_id":[fake.uuid4() for _ in range(n)],
            "customer_id":[random.choice(customers['customer_id'].tolist()) for _ in range (n)],
            "order_date":[fake.date_this_year() for _ in range(n)]
        }
        return pd.DataFrame(data)

    def generate_products():
        categories = ['electronics','books','clothing','food','toys']
        data={
            "product_id":[fake.uuid4() for _ in range(50)],
            "product_name":[fake.word().capitalize() for _ in range (50)],
            "category":[random.choice(categories) for _ in range(50)],
            "price":[random.uniform(5,500) for _ in range(50)]
        }
        return pd.DataFrame(data)
    
    def generate_order_details(orders,products,n=500):
        
        data={
            "order_detail_id":[fake.uuid4() for _ in range(n)],
            "order_id":[random.choice(orders["order_id"].tolist()) for _ in range (n)],
            "product_id":[random.choice(products["product_id"].tolist()) for _ in range(n)],
            "quantity":[random.randint(1,5) for _ in range(n)],
            "price":[random.uniform(5,500) for _ in range(n)]
        }
        return pd.DataFrame(data)

    def generate_payments(orders,n=200):
        
        data={
            "payment_id":[fake.uuid4() for _ in range(n)],
            "order_id":[random.choice(orders["order_id"].tolist()) for _ in range (n)],
            "payment_date":[fake.date_this_year() for _ in range(n)],
            "amount":[random.uniform(10,1000) for _ in range(n)],
            "payment_method":[random.choice(["CREDIT CARD","PAYPAL","BANK TRANSFER"]) for _ in range(n)]

        }
        return pd.DataFrame(data)

    def generate_loyalty_programs(customers):
        
        data={
            "loyalty_id":[fake.uuid4() for _ in range(len(customers))],
            "customer_id":customers["customer_id"].tolist(),
            "points":[random.randint(0,1000) for _ in range(len(customers))],
            "level":[random.choice(["Bronze","silver","gold","platinium"]) for _ in range(len(customers))]

        }
        return pd.DataFrame(data)

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

    customers = generate_customers()
    orders = generate_orders(customers)
    products = generate_products()
    order_details = generate_order_details(orders, products)
    payments = generate_payments(orders)
    loyalty_programs = generate_loyalty_programs(customers)

    customers.to_sql('customers', engine, if_exists='replace', index=False)
    orders.to_sql('orders', engine, if_exists='replace', index=False)
    products.to_sql('products', engine, if_exists='replace', index=False)
    order_details.to_sql('order_details', engine, if_exists='replace', index=False)
    payments.to_sql('payments', engine, if_exists='replace', index=False)
    loyalty_programs.to_sql('loyalty_programs', engine, if_exists='replace', index=False)

    return customers, orders, products, order_details, payments, loyalty_programs

def extract_data_from_db():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    
    customers = pd.read_sql('SELECT * FROM customers', engine)
    orders = pd.read_sql('SELECT * FROM orders', engine)
    products = pd.read_sql('SELECT * FROM products', engine)
    order_details = pd.read_sql('SELECT * FROM order_details', engine)
    payments = pd.read_sql('SELECT * FROM payments', engine)
    loyalty_programs = pd.read_sql('SELECT * FROM loyalty_programs', engine)
    
    return customers, orders, products, order_details, payments, loyalty_programs    

