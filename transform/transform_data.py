def transform_data(df):
    df['email']=df['email'].str.lower()
    return df