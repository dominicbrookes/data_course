from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
 
with DAG(
    dag_id="britmart_dependency_fix",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
) as dag:
 
    extract = BashOperator(
        task_id="extract",
        bash_command="echo extract",
    )
 
    transform = BashOperator(
        task_id="transform",
        bash_command="echo transform",
    )
 
    validate = BashOperator(
        task_id="validate",
        bash_command="echo validate",
    )
 
    extract >> transform >> validate