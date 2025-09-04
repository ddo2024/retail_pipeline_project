import pandas as pd
import sqlalchemy
import pymysql
import psycopg2

def transfer_all_tables(chunksize=1000, copy_chunk = 200):
    # Connect to MySQL
    mysql_engine = sqlalchemy.create_engine("mysql+pymysql://root:rootpass@retail-mysql:3306/retail_db")

    # Connect to PostgreSQL
    postgres_engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:pgpass@retail-postgres:5432/retail_dw")

    # List of tables to transfer
    tables = ['customers', 'products', 'employees', 'stores', 'categories', 'brands', 'suppliers', 'loyalty_programs', 'sales_transactions', 'sales_items', 'payments', 'inventory', 'stock_movements', 'purchase_orders', 'shipments', 'returns', 'customer_feedback', 'store_visits', 'pricing_history', 'promotions', 'campaigns', 'discount_rules', 'tax_rules']

    for table in tables:
        print(f"Transferring table: {table}")

        try:
            # Read data from MySQL
            df = pd.read_sql_table(table, con=mysql_engine)

            # Write to PostgreSQL
            df.to_sql(table, con=postgres_engine, if_exists='append', index=False)
            print(f"✅ Successfully transferred {table}")

        except Exception as e:
            print(f"❌ Failed to transfer {table}: {e}")
if __name__ == "__main__":
    transfer_all_tables()
