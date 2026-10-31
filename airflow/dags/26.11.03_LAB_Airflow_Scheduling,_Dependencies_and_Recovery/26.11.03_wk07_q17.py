"""
Optional Practice

Q. 17 — Build a Scheduled DAG with Retry

Create a complete DAG: dag_id = britmart_daily_orders, schedule = @daily, catchup = False

Tasks: extract, validate

Requirements: extract retries = 2, retry delay = 2 minutes, extract -> validate
"""

from airflow.sdk import DAG
from airflow.sdk import BaseHook, Variable

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor 

from datetime import datetime, timedelta
from pathlib import Path
# Imported all the libaraies I need for this lab, not sure thats best practise.

with DAG(
    dag_id = 'britmart_daily_orders',
    start_date = datetime(2026, 10, 30),
    schedule = "@daily",
    catchup = False,
    # default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

    extract = BashOperator(
            task_id = "extract", 
            bash_command = "echo 'Extract.'",
            retries = 2,
            retry_delay = timedelta(minutes=2)
    )

    validate = BashOperator(task_id = "validate", bash_command = "echo 'Validate.'")

    extract >> validate