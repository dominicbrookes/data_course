"""
Q. 5 — Create a Manual-Run DAG
Create a complete DAG:

dag_id = brightcart_manual_pipeline
schedule = None
catchup = False

Add one task:

run_pipeline
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
    dag_id = "brightcart_manual_pipeline",
    start_date = datetime(2026, 11, 7),
    schedule = None,
    catchup = False,
    #default_args = default_args,
    #max_active_runs = 1
    tags = ["week8_day1", "airflow"]
) as dag:

    run_pipeline = BashOperator(task_id = "run_pipeline", bash_command = ("echo 'Run pipeline.'"))
