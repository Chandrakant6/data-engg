import pandas as pd
from sqlalchemy import create_engine

# ---------- Step 1: Read the merged CSV ----------
merged_df = pd.read_csv("data/merged_output.csv")

# ---------- Step 2: (Optional minimal cleaning) ----------
merged_df.drop_duplicates(inplace=True)
merged_df = merged_df.fillna(value=pd.NA)

# ---------- Step 3: Connect to PostgreSQL ----------
# Example credentials — change these to your local setup
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "data_engg"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ---------- Step 4: Load the data ----------
TABLE_NAME = "merged_data"

merged_df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"✅ Successfully loaded {len(merged_df)} rows into '{TABLE_NAME}' table in PostgreSQL!")
