"""
Part 2 — Task States, Retries, and Retry Delay

Q. 5 — Add a Retry to a Task

Create a complete DAG with one task: load_orders

Requirements: retries = 2, retry_delay = 1 minute
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
# 'block automatically picks up that DAG’s default_args. That’s what the indentation does: it puts load_orders inside the DAG, so the DAG hands its defaults to it.'

default_args = {"retries": 2, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id = "q05",
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

    load_orders = BashOperator(task_id = "load_orders", bash_command = "echo 'Orders loaded.'")
# Somehow, airflow knows to use the default arguments without me listing them below the dag.

"""
Or, with no defaul arguments...

with DAG(
    dag_id = "q05",
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    # default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

    load_orders = BashOperator(
        task_id = "load_orders", 
        bash_command = "echo 'Orders loaded.'",
        retries = 2,
        retry_delay = timedelta(minutes=1) 
    )
"""