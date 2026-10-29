from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = "q07",
    start_date = datetime(2026, 10, 28),
    schedule = None,
    catchup = False,
) as dag:

    extract = BashOperator(
        task_id = "extract",
        bash_command = "echo 'Extract.'")

    clean = BashOperator(
        task_id = "clean",
        bash_command = "echo 'Clean.'")

    report = BashOperator(
        task_id = "report",
        bash_command = "echo 'Report.'")

    extract >> clean >> report