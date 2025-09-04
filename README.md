PROJECT OVERVIEW

This project builds an end-to-end ELT pipeline for retail analytics.
It ingests raw data from MySQL, transfers it into PostgreSQL, cleans and transforms it with DBT, orchestrates workflows with Airflow, and provides business insights via Metabase



⚙️ Prerequisites

Docker & Docker Compose installed

Git installed

🚀 Setup Instructions

Clone repo

git clone <repo_url>
cd retail_pipeline_project


Start Docker containers

docker-compose up -d


Create schema in MySQL

docker exec -i retail-mysql mysql -uroot -prootpass retail_db < ddl/schema.sql


Load raw data into MySQL
Place CSV files in /data and run the loader script (or Airflow task load_csv_to_mysql).

Run Airflow DAG

# Access Airflow UI
open http://localhost:8080


Trigger retail_elt_pipeline from the Airflow dashboard.

Run DBT transformations

docker exec -it retail-dbt dbt run

docker exec -it retail-dbt dbt test

📊 Visualization with Metabase

Open Metabase

open http://localhost:3000


Complete the setup wizard.

Add a new database connection:

Database Type: PostgreSQL
Host: retail-postgres
Port: 5432
Database Name: retail_dw
Username: postgres
Password: postgres


Build dashboards using tables in the gold layer.

🔍 Testing the Pipeline

Insert a new record into MySQL:

docker exec -it retail-mysql mysql -uroot -prootpass retail_db

INSERT INTO sales_transactions (transaction_id, customer_id, store_id, employee_id, transaction_date, total_amount, payment_id)
VALUES (9999, 1, 1, 1, NOW(), 150.00, 1);


Re-run the Airflow DAG.

Check transformed results in Postgres (gold tables) and confirm in Metabase dashboards.

🛠 Troubleshooting

Airflow UI not loading

docker logs retail_pipeline_project-airflow-webserver-1


DBT errors

docker exec -it retail-dbt dbt debug


Postgres connection refused

lsof -i :5432


Kill the process using port 5432 if needed, then restart Docker.