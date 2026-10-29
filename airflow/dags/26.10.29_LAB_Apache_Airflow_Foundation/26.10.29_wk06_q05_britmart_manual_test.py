from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="britmart_manual_test",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
) as dag:

    show_message = BashOperator(
        task_id="show_message",
        bash_command="echo 'BritMart manual test'",
    )