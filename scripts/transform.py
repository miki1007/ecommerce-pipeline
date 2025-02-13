import pandas as pd
import os
import sys
import traceback

def clean_and_transform():
    try:
        # Paths setup
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir, processed_dir = os.path.join(project_root, "data"), os.path.join(project_root, "data", "processed")
        os.makedirs(processed_dir, exist_ok=True)

        # Required files
        required_files = {
            "orders": "olist_orders_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "products": "olist_products_dataset.csv",
            "customers": "olist_customers_dataset.csv"
        }

        # Validate and load data
        raw_data = {
            name: pd.read_csv(os.path.join(data_dir, filename))
            for name, filename in required_files.items()
            if os.path.exists(os.path.join(data_dir, filename))
        }

        if len(raw_data) != len(required_files):
            missing = [f for f in required_files.values() if f not in raw_data]
            raise FileNotFoundError(f"Missing files: {missing}")

        # Data Cleaning
        orders = raw_data["orders"].dropna(subset=["order_purchase_timestamp"])
        orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])

        products = raw_data["products"].fillna({"product_category_name": "Unknown"})

        order_details = raw_data["order_items"].merge(products, on="product_id", how="left")

        customers = raw_data["customers"]
        customers["customer_state"] = customers["customer_state"].str.upper()

        # Save cleaned data
        processed_data = {
            "fact_orders": orders,
            "fact_order_details": order_details,
            "dim_products": products,
            "dim_customers": customers
        }

        for name, df in processed_data.items():
            df.to_parquet(os.path.join(processed_dir, f"{name}.parquet"), index=False)

        print("\n✅ Data cleaning and transformation complete!")

    except Exception:
        print("\n❌ Error occurred during processing:\n", traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    clean_and_transform()