import pandas as pd
import numpy as np
import random
import os

# -----------------------
# SETTINGS
# -----------------------
N_CUSTOMERS = 3000
N_PRODUCTS = 200
N_ORDERS = 10000
N_ORDER_ITEMS = 20000

random.seed(42)
np.random.seed(42)

# -----------------------
# CUSTOMERS
# -----------------------
def random_channel():
    return random.choice(["paid_search", "organic", "affiliate", "social", "email"])

countries = ["US", "UK", "CA", "AU", "DE", "MX", "IN"]

customers = pd.DataFrame({
    "customer_id": range(1, N_CUSTOMERS + 1),
    "first_name": [f"Cust{i}" for i in range(1, N_CUSTOMERS + 1)],
    "last_name": [f"Last{i}" for i in range(1, N_CUSTOMERS + 1)],
    "email": [f"user{i}@example.com" for i in range(1, N_CUSTOMERS + 1)],
    "created_ts": pd.date_range(start="2020-01-01", periods=N_CUSTOMERS, freq="H"),
    "country": np.random.choice(countries, N_CUSTOMERS),
    "marketing_channel": [random_channel() for _ in range(N_CUSTOMERS)]
})

# -----------------------
# PRODUCTS
# -----------------------
categories = ["Electronics", "Beauty", "Home", "Sports", "Fashion", "Books"]
products = pd.DataFrame({
    "product_id": range(1, N_PRODUCTS + 1),
    "product_name": [f"Product{i}" for i in range(1, N_PRODUCTS + 1)],
    "category": np.random.choice(categories, N_PRODUCTS),
    "brand": [f"Brand{i % 20}" for i in range(1, N_PRODUCTS + 1)],
    "current_price": np.random.uniform(10, 300, size=N_PRODUCTS),
    "cost": np.random.uniform(5, 200, size=N_PRODUCTS)
})

# -----------------------
# ORDERS
# -----------------------
order_dates = pd.date_range("2021-01-01", "2023-01-01", freq="H")
orders = pd.DataFrame({
    "order_id": range(1, N_ORDERS + 1),
    "customer_id": np.random.randint(1, N_CUSTOMERS, N_ORDERS),
    "order_ts": np.random.choice(order_dates, N_ORDERS),
    "order_status": np.random.choice(["completed", "shipped", "cancelled"], N_ORDERS, p=[0.75, 0.20, 0.05]),
    "currency": "USD",
    "total_amount": np.random.uniform(20, 600, size=N_ORDERS)
})

# -----------------------
# ORDER ITEMS
# -----------------------
order_items = pd.DataFrame({
    "order_item_id": range(1, N_ORDER_ITEMS + 1),
    "order_id": np.random.randint(1, N_ORDERS, N_ORDER_ITEMS),
    "product_id": np.random.randint(1, N_PRODUCTS, N_ORDER_ITEMS),
    "quantity": np.random.randint(1, 5, N_ORDER_ITEMS),
    "unit_price": np.random.uniform(10, 200, size=N_ORDER_ITEMS),
    "discount_amount": np.random.uniform(0, 20, size=N_ORDER_ITEMS)
})

# -----------------------
# SAVE TO DBFS PATHS (Databricks) OR LOCAL
# -----------------------
base_path = "/Volumes/workspace/default/course_data/project/"
os.makedirs(base_path, exist_ok=True)

customers.to_csv(os.path.join(base_path, "raw_customers.csv"), index=False)
products.to_csv(os.path.join(base_path, "raw_products.csv"), index=False)
orders.to_csv(os.path.join(base_path, "raw_orders.csv"), index=False)
order_items.to_csv(os.path.join(base_path, "raw_order_items.csv"), index=False)

print("Raw data created in /dbfs/FileStore/tables.")
