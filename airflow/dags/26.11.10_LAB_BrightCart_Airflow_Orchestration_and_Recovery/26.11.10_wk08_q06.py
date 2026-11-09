"""
Q. 6 — Create a Daily Scheduled DAG
Create a complete DAG:

dag_id = brightcart_daily_pipeline
start_date = 2026-08-01
schedule = @daily
catchup = False

Add:

extract
publish

with:

extract -> publish
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
    dag_id="brightcart_daily_pipeline",
    start_date=datetime(2026, 11, 7),
    schedule="@daily",
    catchup=False,
    #default_args = default_args,
    #max_active_runs = 1,
    tags = ["week8_day1", "airflow"]
) as dag:
 
    extract = BashOperator(task_id="extract", bash_command="echo extract")

    publish = BashOperator(task_id="publish", bash_command="echo publish")
 
    extract >> publish