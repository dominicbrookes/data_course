"""
Part 3 — Retries, Failure Propagation, Parse Errors, and Runtime Errors
Q. 9 — Add a Controlled Retry
Create a complete DAG with:

download_file
process_file

Requirements:

download_file retries = 2
retry delay = 1 minute
download_file -> process_file
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
    dag_id = "q09",
    start_date = datetime(2026, 11, 7),
    schedule = None,
    catchup = False,
    #default_args = default_args
    #max_active_runs = 1,
    tags = ["week8_day1", "airflow"]
) as dag:

    download_file = BashOperator(task_id = "download_file", bash_command = "echo 'Download File.'", retries = 2, retry_delay = timedelta(minutes=1))

    process_file = BashOperator(task_id = "process_file", bash_command = "echo 'Process File.'")

    download_file >> process_file