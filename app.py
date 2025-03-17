import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# API Endpoint
API_URL = "https://api.example.com/sales"

# PostgreSQL Database Connection Details
DB_HOST = "localhost"
DB_NAME = "sales_db"
DB_USER = ""
DB_PASSWORD = "24081314"
DB_PORT = "5432"

# Extract: Fetch sales data from API
def extract_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()  
    else:
        print("Error fetching data from API")
        return []

# Transform: Convert to DataFrame and clean data
def transform_data(data):
    df = pd.DataFrame(data)
    df["sale_date"] = pd.to_datetime(df["sale_date"])  
    df["total_price"] = df["quantity"] * df["unit_price"]  
    df = df[["sale_id", "product", "quantity", "unit_price", "total_price", "sale_date"]]  
    return df

# Load: Insert data into PostgreSQL
def load_data(df):
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    with engine.connect() as conn:
        df.to_sql("sales_data", conn, if_exists="replace", index=False) 
    print("Data successfully loaded into PostgreSQL")

# Run ETL Pipeline
def etl_pipeline():
    data = extract_data()
    if data:
        df = transform_data(data)
        load_data(df)
    else:
        print("No data extracted")

if __name__ == "__main__":
    etl_pipeline()
