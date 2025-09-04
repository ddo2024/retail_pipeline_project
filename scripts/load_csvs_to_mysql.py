import pandas as pd
from sqlalchemy import create_engine
import os

def load_csvs_to_mysql():
    # Connect to MySQL
    engine = create_engine("mysql+pymysql://root:rootpass@retail-mysql:3306/retail_db")

    tables = ['customers', 'products', 'employees', 'stores', 'categories', 'brands', 'suppliers', 'loyalty_programs', 'sales_transactions', 'sales_items', 'payments', 'inventory', 'stock_movements', 'purchase_orders', 'shipments', 'returns', 'customer_feedback', 'store_visits', 'pricing_history', 'promotions', 'campaigns', 'discount_rules', 'tax_rules']

    for table in tables:
        file_path = f"/app/data/{table}.csv"
        if os.path.exists(file_path):
            try:
                print(f"📥 Loading {file_path} into MySQL table '{table}'...")
                df = pd.read_csv(file_path)
                df.to_sql(table, con=engine, if_exists='append', index=False)
                print(f"✅ Successfully loaded '{table}'")
            except Exception as e:
                print(f"❌ Failed to load '{table}': {e}")
        else:
            print(f"⚠️ File not found: {file_path}")
if __name__ == "__main__":
    load_csvs_to_mysql()
