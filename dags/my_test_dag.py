from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id = 'my_test_dag',
    start_date = datetime(2023,9,3)
) as dag:
    task_1 = BashOperator(
        task_id = 'SayHello',
        bash_command = "echo 'Hello World!'"
    )
    
    task_2 = BashOperator(
        task_id = 'SayGoodBye',
        bash_command = "echo 'Good Bye!'"
    )
    
task_1 >> task_2