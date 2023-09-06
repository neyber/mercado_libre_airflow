from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'neyber',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

# Define the DAG
dag = DAG(
    dag_id = 'test_pg',
    default_args = default_args,
    start_date = datetime(2023, 9, 3),
    schedule_interval = '0 0 * * *'
)

# Define the task using PostgresOperator
task_1 = PostgresOperator(
    task_id = "create_table_task",
    postgres_conn_id = 'pg_localhost',
    sql = """
            CREATE TABLE IF NOT EXISTS test (
                id INT,
                name VARCHAR(100)
            )
        """,
    dag = dag
)
