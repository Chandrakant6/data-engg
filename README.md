.

📦 Data Engineering Pipeline — Prefect + PostgreSQL + Grafana
🧩 Overview

This project demonstrates an end-to-end data engineering pipeline that:

Ingests and transforms raw order data

Loads it into PostgreSQL

Orchestrates execution every hour using Prefect

Visualizes results through Grafana dashboards

⚙️ Tools Used

Python 3.12

Prefect 2.x (for orchestration)

PostgreSQL (for data storage)

Grafana (for visualization)

Pandas + SQLAlchemy (for data cleaning & loading)

🧠 Pipeline Summary

Data Ingestion → Loads raw data into PostgreSQL (merged_data)

Analytics Creation → Builds:

sales_summary

delivery_performance_summary

Prefect Flow → Runs the ETL pipeline every hour

Grafana Dashboards → Visualize insights like:

Revenue by payment type

Orders by city

Average delivery performance

Total orders over time


▶️ How to Run

Set up environment

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


Start PostgreSQL

sudo systemctl start postgresql


Run Prefect Flow

python3 -m flow.main_flow


Start Prefect UI (optional)

prefect server start


Open Grafana

URL: http://localhost:3000

Add PostgreSQL as a data source

Import dashboard panels and run your queries