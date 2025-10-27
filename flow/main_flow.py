from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import requests
import sqlalchemy

# --- CONFIG ---
DB_URL = "postgresql://postgres:postgres@localhost:5432/data_engg"
CUSTOMERS_API = "http://127.0.0.1:8000/customers?chunk=1"
ITEMS_CSV = "data/items.csv"
ORDERS_CSV = "data/orders.csv"
PAYMENTS_CSV = "data/payments.csv"


# --- TASKS ---

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_customers():
    """Fetch customer data from FastAPI endpoint."""
    response = requests.get(CUSTOMERS_API)
    data = response.json()["data"]
    df = pd.DataFrame(data)
    print(f"✅ Customers fetched: {len(df)}")
    return df


@task
def read_csv_files():
    """Read local CSV files."""
    items = pd.read_csv(ITEMS_CSV)
    orders = pd.read_csv(ORDERS_CSV)
    payments = pd.read_csv(PAYMENTS_CSV)
    print("✅ CSV files loaded")
    return items, orders, payments


@task
def merge_data(customers, items, orders, payments):
    """Merge all data sources."""
    merged = orders.merge(customers, on="customer_id", how="left")
    merged = merged.merge(payments, on="order_id", how="left")
    merged = merged.merge(items, on="order_id", how="left")
    print(f"✅ Merged rows: {len(merged)}")
    return merged


@task
def clean_and_load_to_postgres(df):
    """Clean & load merged data into PostgreSQL."""
    engine = sqlalchemy.create_engine(DB_URL)
    df.fillna("", inplace=True)
    df.to_sql("merged_data", engine, if_exists="replace", index=False)
    print("✅ Data loaded into PostgreSQL!")


# --- MAIN FLOW ---
@flow(name="Data Pipeline Flow", retries=2, retry_delay_seconds=60)
def main_flow():
    """End-to-end Prefect data pipeline (hourly run)."""
    customers = fetch_customers()
    items, orders, payments = read_csv_files()
    merged = merge_data(customers, items, orders, payments)
    clean_and_load_to_postgres(merged)


if __name__ == "__main__":
    # Serve flow every 1 hour using Prefect 3.x syntax
    main_flow.serve(
        name="hourly_data_pipeline",
        cron="0 * * * *",  # every hour
    )
