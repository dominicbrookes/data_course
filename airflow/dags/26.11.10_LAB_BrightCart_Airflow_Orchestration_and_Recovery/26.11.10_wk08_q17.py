"""
Q. 17 — Build a Complete Recovery-Aware DAG
Create a complete DAG with:

extract
clean
build_report
validate_output

Requirements:

schedule = @daily
catchup = False
extract retries = 2
clean retries = 1
dependency chain:
extract -> clean -> build_report -> validate_output
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
    dag_id="q17",
    start_date=datetime(2026, 11, 7),
    schedule="@daily",
    catchup=False,
    #default_args = default_args
    #max_active_runs = 1,
    tags = ["week8_day1", "airflow"]
) as dag:
 
    extract = BashOperator(task_id="extract", bash_command="echo extract", retries=2, retry_delay=timedelta(minutes=1))
 
    clean = BashOperator(task_id="clean", bash_command="echo clean", retries=1, retry_delay=timedelta(minutes=1))
 
    build_report = BashOperator(task_id="build_report", bash_command="echo report")
 
    validate_output = BashOperator(task_id="validate_output", bash_command="echo validate")
 
    extract >> clean >> build_report >> validate_output