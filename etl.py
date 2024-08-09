from extract.extract_data import generate_sample_data, extract_data_from_db
from transform.transform_data import transform_data
from load.load_data import load_data_to_postgres, save_dataframe_to_excel, upload_to_s3, upload_to_gcp
from config import S3_BUCKET_NAME, GCP_BUCKET_NAME

def etl_pipeline():
    # Generate sample data and load to DB
    sample_data = generate_sample_data()
    load_data_to_postgres(sample_data)
    print("Sample data loaded to PostgreSQL")

    # Extract data from DB
    data = extract_data_from_db()
    print("Data extracted from PostgreSQL")

    # Transform data
    transformed_data = transform_data(data)
    print("Data transformed")

    # Save to Excel
    excel_data = save_dataframe_to_excel(transformed_data, 'transformed_data.xlsx')

    # Upload to S3
    upload_to_s3(excel_data, S3_BUCKET_NAME, 'transformed_data.xlsx')

    # Upload to GCP
    upload_to_gcp(excel_data, GCP_BUCKET_NAME, 'transformed_data.xlsx')

    print("Transformed data uploaded to S3 and GCP")

if __name__ == '__main__':
    etl_pipeline()