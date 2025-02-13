import pandas as pd
import os

# Path to your dataset folder (update this if needed)
data_dir = os.path.expanduser("~/ecommerce-pipeline/data")

# Load CSV files
files = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "customers": "olist_customers_dataset.csv"
}

dfs = {name: pd.read_csv(os.path.join(data_dir, file)) for name, file in files.items()}

# Test: Print orders data summary
print("Orders Data Sample:")
print(dfs["orders"].head())