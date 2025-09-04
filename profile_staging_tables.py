import pandas as pd
from sqlalchemy import create_engine

pg_engine = create_engine("postgresql+psycopg2://postgres:pgpass@localhost:5432/retail_dw")



try:
    with pg_engine.connect() as conn:
        print("✅ PostgreSQL connection successful")
except Exception as e:
    print("❌ Failed to connect:", e)


tables = [
    "stg_customers", "stg_products", "stg_employees", "stg_stores", "stg_categories",
    "stg_brands", "stg_suppliers", "stg_loyalty_programs", "stg_sales_transactions",
    "stg_sales_items", "stg_payments", "stg_inventory", "stg_stock_movements",
    "stg_purchase_orders", "stg_shipments", "stg_returns", "stg_customer_feedback",
    "stg_store_visits", "stg_pricing_history", "stg_promotions", "stg_campaigns",
    "stg_discount_rules", "stg_tax_rules"
]

for table in tables:
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", pg_engine)
        nulls = df.isnull().sum()
        na_values = (df == "N/A").sum()
        print(f"\n📊 {table}")
        print("Nulls:", nulls[nulls > 0].to_dict())
        print("N/A values:", na_values[na_values > 0].to_dict())

        if 'age' in df.columns:
            bad_age = df[(df['age'] < 0) | (df['age'] > 120)]
            print("⚠️ Bad age values:", len(bad_age))

        if 'total_amount' in df.columns:
            bad_amount = df[df['total_amount'] < 0]
            print("⚠️ Negative total_amount values:", len(bad_amount))

        if 'transaction_date' in df.columns:
            future_dates = df[df['transaction_date'] > pd.Timestamp.today()]
            print("⚠️ Future transaction dates:", len(future_dates))

    except Exception as e:
        print(f"❌ Failed to profile {table}: {e}")

