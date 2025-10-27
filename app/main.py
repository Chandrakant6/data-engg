from fastapi import FastAPI
import pandas as pd

app = FastAPI(title="Data API")

# Load the CSV once when the app starts
data = pd.read_csv("data/customers.csv")

@app.get("/")
def root():
    return {"message": "Welcome to the Data API!"}

@app.get("/customers")
def get_customers():
    return data.to_dict(orient="records")
