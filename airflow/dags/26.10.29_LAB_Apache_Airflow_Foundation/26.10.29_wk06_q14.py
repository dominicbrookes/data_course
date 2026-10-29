from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
 
with DAG(
    dag_id="britmart_complete_dag_file",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
) as dag:
 
    start = BashOperator(
        task_id="start",
        bash_command="echo start",
    )
 
    process = BashOperator(
        task_id="process",
        bash_command="echo process",
    )
 
    finish = BashOperator(
        task_id="finish",
        bash_command="echo finish",
    )
 
    start >> process >> finish
