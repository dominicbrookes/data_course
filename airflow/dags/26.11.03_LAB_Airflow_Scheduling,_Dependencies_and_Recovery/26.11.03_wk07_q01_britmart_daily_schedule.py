"""
Part 1 — Schedule, Start Date, Catchup, and Backfill

Q. 1 — Create a Daily Scheduled DAG

Create a DAG named: britmart_daily_schedule

Requirements: start_date = 2026-08-01, schedule = daily, catchup = False, one task named show_run

The task should print: daily run
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
    dag_id="britmart_daily_schedule",
    start_date = datetime(2026, 10, 30),
    schedule = "@daily", # cron code is '0 0 * * *', and this will run at midnight.
    catchup = False,
    # default_args = default_args ... Manually created definition.
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:
# Adding in extra options from the lecture, just so I know they exist.

    show_run = BashOperator(task_id = "show_run", bash_command = "echo 'daily run'")