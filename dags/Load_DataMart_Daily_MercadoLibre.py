from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from mercado_libre_api import fetch_categories_data, insert_data_to_postgres

default_args = {
    'owner': 'neyber'
}

dag = DAG(
    dag_id = 'Load_DataMart_Daily_MercadoLibre',
    default_args = default_args,
    start_date = datetime(2023, 9, 5)
)

task_1 = PostgresOperator(
    task_id = 'create_staging_table',
    postgres_conn_id = 'pg_localhost',
    sql = """
        CREATE TABLE IF NOT EXISTS dmstage.Categories (
            id VARCHAR(50),
            name VARCHAR(100),
            load_date TIMESTAMP,
            hash_key CHAR(32)
        )
    """,
    dag = dag
)

task_2 = PythonOperator(
    task_id = 'fetch_categories_data',
    python_callable = fetch_categories_data,
    dag = dag
)

task_3 = PostgresOperator(
    task_id = 'truncate_staging_table',
    postgres_conn_id = 'pg_localhost',
    sql = """
        TRUNCATE TABLE dmstage.categories
    """,
    dag = dag
)
    
task_4 = PythonOperator(
    task_id = 'load_stg_data',
    python_callable = insert_data_to_postgres,
    dag = dag
)

task_1 >> task_2 >> task_3 >> task_4