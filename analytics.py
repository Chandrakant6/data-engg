import sqlalchemy

# PostgreSQL connection details
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "data_engg"
DB_HOST = "localhost"
DB_PORT = "5432"

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---- SQL QUERIES ----

sales_summary_query = """
CREATE TABLE IF NOT EXISTS sales_summary AS
SELECT
    customer_unique_id,
    customer_city,
    customer_state,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(NULLIF(payment_value, '')::numeric) AS total_revenue,
    AVG(NULLIF(payment_value, '')::numeric) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM merged_data
WHERE order_status = 'delivered'
GROUP BY customer_unique_id, customer_city, customer_state;
"""

delivery_performance_query = """
CREATE TABLE IF NOT EXISTS delivery_performance_summary AS
SELECT
    order_id,
    customer_unique_id,
    customer_city,
    customer_state,
    (order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp) AS actual_delivery_time,
    (order_estimated_delivery_date::timestamp - order_purchase_timestamp::timestamp) AS estimated_delivery_time,
    (order_delivered_customer_date::timestamp - order_estimated_delivery_date::timestamp) AS delay_time
FROM merged_data
WHERE
    order_delivered_customer_date IS NOT NULL
    AND order_purchase_timestamp IS NOT NULL
    AND order_estimated_delivery_date IS NOT NULL
    AND order_delivered_customer_date <> ''
    AND order_purchase_timestamp <> ''
    AND order_estimated_delivery_date <> '';
"""

# ---- EXECUTE QUERIES ----
with engine.connect() as conn:
    print("Creating analytics tables...")
    conn.execute(sqlalchemy.text(sales_summary_query))
    conn.execute(sqlalchemy.text(delivery_performance_query))
    print("✅ Both analytics tables created successfully!")
