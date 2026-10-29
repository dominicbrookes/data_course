from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="britmart_four_task_pipeline",
    start_date=datetime(2026, 10, 29),
    schedule=None,
    catchup=False,
) as dag:
 
    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'Extract.'",
    )
 
    validate = BashOperator(
        task_id="validate",
        bash_command="echo 'Validate.'",
    )
 
    transform = BashOperator(
        task_id="transform",
        bash_command="echo 'Transform.'",
    )
 
    publish = BashOperator(
        task_id="publish",
        bash_command="echo 'Publish.'",
    )
 
    extract >> validate >> transform >> publish
