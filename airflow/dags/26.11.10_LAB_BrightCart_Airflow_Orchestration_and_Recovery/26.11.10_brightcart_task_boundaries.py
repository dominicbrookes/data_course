"""
Q. 2 — Build a DAG with Clear Task Boundaries
Create a complete DAG named:

brightcart_task_boundaries

read_source
clean_rows
build_daily_sales
validate_output

Use BashOperator and the correct dependency chain.
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
    dag_id = "q02",
    start_date = datetime(2026, 11, 7),
    schedule = None,
    catchup = False,
    #default_args = default_args,
    #max_active_runs = 1,
    tags = ["week8_day1", "airflow"]
) as dag:

    read_source = BashOperator(task_id = "read_source", bash_command = "echo 'Read source.'")

    clean_rows = BashOperator(task_id = "clean_rows", bash_command = "echo 'Clean rows.'")

    build_daily_sales = BashOperator(task_id ="build_daily_sales", bash_command = "echo 'Build daily sales.'")

    validate_output = BashOperator(task_id ="validate_output", bash_command = "echo 'Validate output.'")

    read_source >> clean_rows >> build_daily_sales >> validate_output