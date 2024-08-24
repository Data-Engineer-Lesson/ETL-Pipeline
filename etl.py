from extract.extract_data import load_sample_data_to_db, extract_data_from_db
from transform.transform_data import transform_data
from load.load_data import save_dataframe_to_csv, save_dataframe_to_excel, upload_to_gcp
from config import GCP_BUCKET_NAME

def etl_pipeline():
    # Sample data oluştur ve veritabanına yükle
    customers, orders, products, order_details, payments, loyalty_programs = load_sample_data_to_db()
    print("Sample data loaded to PostgreSQL")

    # Extract edilen verileri CSV olarak GCP'ye yükle
    save_dataframe_to_csv(customers, 'customers.csv', GCP_BUCKET_NAME)
    save_dataframe_to_csv(orders, 'orders.csv', GCP_BUCKET_NAME)
    save_dataframe_to_csv(products, 'products.csv', GCP_BUCKET_NAME)
    save_dataframe_to_csv(order_details, 'order_details.csv', GCP_BUCKET_NAME)
    save_dataframe_to_csv(payments, 'payments.csv', GCP_BUCKET_NAME)
    save_dataframe_to_csv(loyalty_programs, 'loyalty_programs.csv', GCP_BUCKET_NAME)

    # Veritabanından verileri çıkar
    data = extract_data_from_db()
    print("Data extracted from PostgreSQL")

    # Veriyi dönüştür
    transformed_data = transform_data(*data)
    print("Data transformed")

    # Transform edilen verileri CSV olarak GCP'ye yükle
    transformed_file_names = ['final_customers.csv', 'category_sales.csv', 'payment_summary.csv']
    for df, file_name in zip(transformed_data, transformed_file_names):
        save_dataframe_to_csv(df, file_name, GCP_BUCKET_NAME)

    print("All data uploaded to GCP")

if __name__ == '__main__':
    etl_pipeline()