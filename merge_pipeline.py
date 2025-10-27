import pandas as pd
import requests

# ---------- Step 1: Load CSV data ----------
orders = pd.read_csv("data/orders.csv")
payments = pd.read_csv("data/payments.csv")

# ---------- Step 2: Fetch JSON data from API ----------
CUSTOMERS_API_URL = "http://127.0.0.1:8000/customers"

response = requests.get(CUSTOMERS_API_URL)
if response.status_code != 200:
    raise Exception(f"Failed to fetch customers data: {response.status_code}")

json_data = response.json()

# Extract only the 'data' key
customers_data = json_data.get("data", [])
customers = pd.DataFrame(customers_data)

# ---------- Step 3: Merge datasets ----------
merged_orders_customers = pd.merge(orders, customers, on="customer_id", how="left")
final_merged = pd.merge(merged_orders_customers, payments, on="order_id", how="left")

# ---------- Step 4: Output ----------
print("✅ Merged dataset created successfully!")
print(final_merged.head())

# Optional: Save the merged dataset
final_merged.to_csv("data/merged_output.csv", index=False)
