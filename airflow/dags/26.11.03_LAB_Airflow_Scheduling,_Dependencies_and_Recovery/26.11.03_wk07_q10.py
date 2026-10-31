"""
Q. 10 — Create a Dependency Chain with Recovery in Mind

Create a complete DAG: extract, transform, validate, publish

Use a retry only on extract.

Dependencies: extract -> transform -> validate -> publish
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
    dag_id = "q10",
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    # default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

    extract = BashOperator(task_id = "extract", bash_command = "echo 'Extract.'", retries = 2, retry_delay = timedelta(minutes=1))

    transform = BashOperator(task_id = "transform", bash_command = "echo 'Transform.'")

    validate = BashOperator(task_id = "validate", bash_command = "echo 'Validate.'")

    publish = BashOperator(task_id = "publish", bash_command = "echo 'Publish.'")

    extract >> transform >> validate >> publish