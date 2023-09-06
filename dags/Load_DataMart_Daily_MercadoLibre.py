from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import psycopg2
from mercado_libre_api import fetch_categories_data

default_args = {
    'owner': 'neyber',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

# Define the DAG
dag = DAG(
    dag_id = 'Load_DataMart_Daily_MercadoLibre',
    default_args = default_args,
    start_date = datetime(2023, 9, 4),
    schedule_interval = '0 0 * * *'
)

task_1 = PostgresOperator(
    task_id = 'create_staging_tables',
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
    task_id = 'truncate_staging_tables',
    postgres_conn_id = 'pg_localhost',
    sql = """
            TRUNCATE TABLE dmstage.Categories
        """,
    dag = dag
)

def insert_data_to_postgres():
    data = fetch_categories_data()
    
    conn = psycopg2.connect(
        dbname = 'mercado_libre',
        user = 'airflow',
        password = 'airflow',
        host = 'postgres',
        port = '5432'
    )

    cur = conn.cursor()

    insert_query = """
        INSERT INTO dmstage.Categories (id, name, load_date)
        VALUES (%s, %s, %s);
    """
    
    current_datetime = datetime.now()

    for row in data:
        cur.execute(insert_query, (row['id'], row['name'], current_datetime))

    conn.commit()
    cur.close()
    conn.close()
    
task_4 = PythonOperator(
    task_id = 'load_stg_data',
    python_callable = insert_data_to_postgres,
    dag = dag
)

task_1 >> task_2 >> task_3 >> task_4