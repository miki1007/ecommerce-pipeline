import pandas as pd
import os
import sys
import traceback
from datetime import datetime

def clean_and_transform():
    try:
        # ========== ABSOLUTE PATH SETUP ========== 
        script_path = os.path.abspath(__file__)
        script_dir = os.path.dirname(script_path)
        project_root = os.path.dirname(script_dir)
        data_dir = os.path.join(project_root, "data")
        processed_dir = os.path.join(data_dir, "processed")
        
        print("="*50)
        print(f"🔧 Debugging Information")
        print(f"Script path: {script_path}")
        print(f"Project root: {project_root}")
        print(f"Data directory: {data_dir}")
        print(f"Processed directory: {processed_dir}")
        print("="*50)
        
        # Create processed directory if missing
        os.makedirs(processed_dir, exist_ok=True)
        print(f"✅ Created/Verified processed directory")

        # ========== CSV FILE VALIDATION ========== 
        required_files = {
            "orders": "olist_orders_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "products": "olist_products_dataset.csv",
            "customers": "olist_customers_dataset.csv"
        }

        print("\n📂 Checking CSV files:")
        for name, filename in required_files.items():
            file_path = os.path.join(data_dir, filename)
            print(f"  - {filename}: {'✅ Found' if os.path.exists(file_path) else '❌ Missing'} at {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Critical file missing: {file_path}")

        # ========== LOAD DATA ========== 
        print("\n🚚 Loading CSV files into DataFrames:")
        raw_data = {}
        for name, filename in required_files.items():
            file_path = os.path.join(data_dir, filename)
            raw_data[name] = pd.read_csv(file_path)
            print(f"  - {filename}: Loaded {len(raw_data[name])} rows")

        # ========== DATA CLEANING ========== 
        print("\n🧹 Cleaning Data:")
        
        # Clean Orders
        print("  - Cleaning orders data...")
        orders = raw_data["orders"].copy()
        initial_order_count = len(orders)
        orders = orders.dropna(subset=["order_purchase_timestamp"])
        orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])
        print(f"    Removed {initial_order_count - len(orders)} rows with missing timestamps")

        # Clean Products
        print("  - Cleaning products data...")
        products = raw_data["products"].copy()
        products["product_category_name"] = products["product_category_name"].fillna("Unknown")
        print(f"    Filled {products['product_category_name'].isna().sum()} missing categories")

        # Merge Order Items with Products
        print("  - Merging order items with products...")
        order_details = pd.merge(
            raw_data["order_items"], 
            products, 
            on="product_id", 
            how="left",
            validate="many_to_one"
        )
        print(f"    Merged {len(order_details)} order items")

        # Clean Customers
        print("  - Cleaning customers data...")
        customers = raw_data["customers"].copy()
        customers["customer_state"] = customers["customer_state"].str.upper()
        print("    Standardized state codes to uppercase")

        # ========== DATA VALIDATION ========== 
        print("\n🔍 Data Validation Check:")
        print(f"  - Orders: {len(orders)} rows")
        print(f"  - Order Items: {len(order_details)} rows")
        print(f"  - Products: {len(products)} rows")
        print(f"  - Customers: {len(customers)} rows")

        # ========== SAVE PARQUET FILES ========== 
        print("\n💾 Saving Processed Data:")
        processed_data = {
            "fact_orders": orders,
            "fact_order_details": order_details,
            "dim_products": products,
            "dim_customers": customers
        }

        for name, df in processed_data.items():
            output_path = os.path.join(processed_dir, f"{name}.parquet")
            print(f"  - Saving {name} to: {output_path}")
            
            # Validate DataFrame before saving
            if df.empty:
                raise ValueError(f"{name} DataFrame is empty!")
                
            df.to_parquet(
                output_path,
                engine='pyarrow',  # Force PyArrow engine
                compression='snappy'
            )
            print(f"    ✅ Saved {len(df)} rows as {name}.parquet")

        print("\n🎉 Transformation completed successfully!")

    except Exception as e:
        print("\n❌ Critical Error Occurred:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nStack trace:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    clean_and_transform()