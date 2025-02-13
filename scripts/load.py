# ~/ecommerce-pipeline/scripts/load.py
import pandas as pd
import os
import sys
from sqlalchemy import create_engine

def load_to_postgres():
    try:
        # ========== ABSOLUTE PATH CONFIGURATION ========== 
        # Get the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Move up to project root (ecommerce-pipeline)
        project_root = os.path.dirname(script_dir)
        # Define processed data directory
        processed_dir = os.path.join(project_root, "data", "processed")
        
        print(f"🔍 Project root: {project_root}")
        print(f"📂 Processed data directory: {processed_dir}")

        # ========== POSTGRESQL CONNECTION ========== 
        engine = create_engine('postgresql://mikias:your_password@localhost:5432/ecommerce_pipeline')

        # ========== LOAD PARQUET FILES ========== 
        print("\n📤 Loading data into PostgreSQL...")
        
        # Map table names to Parquet files (UPDATE FILENAMES TO MATCH YOUR FILES)
        tables = {
                "fact_order_items": "fact_order_details.parquet",  # Child table (load first)
                "fact_orders": "fact_orders.parquet",              # Parent table (load after)
                "dim_products": "dim_products.parquet",
                "dim_customers": "dim_customers.parquet"
}

        for table_name, parquet_file in tables.items():
            file_path = os.path.join(processed_dir, parquet_file)
            print(f"\n🔍 Loading {parquet_file} from: {file_path}")
            
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Missing file: {file_path}")
            
            # Read Parquet
            df = pd.read_parquet(file_path)
            print(f"✅ Read {len(df)} rows from {parquet_file}")
            
            # Load to PostgreSQL
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            print(f"✅ Loaded {table_name}")

        print("\n🎉 All data loaded successfully!")

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    load_to_postgres()