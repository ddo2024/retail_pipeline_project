from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    'owner': 'retail_team',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='retail_elt_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or set cron later
    catchup=False,
    tags=['retail', 'elt']
) as dag:

    # Step 1: Load CSVs into MySQL

    load_raw_data = DockerOperator(
        task_id='load_csv_to_mysql',
        image='retail_pipeline_project-elt:latest',
        command='python /app/scripts/load_csvs_to_mysql.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='retail_pipeline_project_retail-network',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/scripts', target='/app/scripts', type='bind'),
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/raw_data', target='/app/data', type='bind'),
        ],
        working_dir='/app/scripts'
    )

    # Step 2: Transfer data from MySQL to PostgreSQL
    transfer_raw_to_postgres = DockerOperator(
        task_id='transfer_mysql_to_postgres',
        image='retail_pipeline_project-elt:latest',
        command='python /app/scripts/mysql_to_postgres_data_loader.py',
        mem_limit="2g", 
        docker_url='unix://var/run/docker.sock',
        network_mode='retail_pipeline_project_retail-network',
        auto_remove=False,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/scripts', target='/app/scripts', type='bind')
        ],
        working_dir='/app/scripts'
    )

    # Step 3: Run DBT transformations
    dbt_transform = DockerOperator(
        task_id='run_dbt',
        image='retail-dbt',
        command='sh -c "dbt deps && dbt run"',
        docker_url='unix://var/run/docker.sock',
        network_mode='retail_pipeline_project_retail-network',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/retail_dbt', target='/usr/app', type='bind'),
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/dbt_config', target='/root/.dbt', type='bind')
        ],
        working_dir='/usr/app'
    )

    # Step 4: Run DBT tests
    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='retail-dbt',
        command='dbt test',
        docker_url='unix://var/run/docker.sock',
        network_mode='retail_pipeline_project_retail-network',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/retail_dbt', target='/usr/app', type='bind'),
            Mount(source='/Users/duydo/Desktop/DE2/retail_pipeline_project/dbt_config', target='/root/.dbt', type='bind')
        ],
        working_dir='/usr/app'
    )

    load_raw_data >> transfer_raw_to_postgres >> dbt_transform >> dbt_test


