"""
Q. 20 — Build a Sensor-to-Processing DAG

Create a complete DAG with: wait_for_orders, process_orders, validate_output

Requirements: wait for /tmp/britmart/daily_orders.csv, sensor timeout = 600 seconds, process_orders retries = 1

dependency chain: wait_for_orders -> process_orders -> validate_output
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
    dag_id = 'q_20',
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    # default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

   wait_for_orders = FileSensor(
           task_id="wait_for_orders",
           filepath="/tmp/britmart/daily_orders.csv",
           fs_conn_id="fs_default",
           poke_interval=30,
           timeout=600,
       ) 

   process_orders = BashOperator(task_id = "process_orders", bash_command = "echo 'Process orders.'", retries = 1, retry_delay = timedelta(minutes=1))

   validate_output = BashOperator(task_id = "validate_output", bash_command = "echo 'Validate.'")

   wait_for_orders >> process_orders >> validate_output

