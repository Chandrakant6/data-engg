import pandas as pd
from sqlalchemy import create_engine

# Read the merged CSV 
merged_df = pd.read_csv("data/merged_output.csv")

# minimal cleaning
merged_df.drop_duplicates(inplace=True)
merged_df = merged_df.fillna(value=pd.NA)

# Connect to PostgreSQL 
# Example credentials —
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "data_engg"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Load the data 
TABLE_NAME = "merged_data"

merged_df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✅ Successfully loaded {len(merged_df)} rows into '{TABLE_NAME}' table in PostgreSQL!")
