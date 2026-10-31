"""
Q. 11 — Build a Simple Branch
BritMart should choose one path based on whether a report is required.

Create a complete DAG using BranchPythonOperator.

Return: build_report

when: report_required = True

Otherwise return: skip_report
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

# Option 1 (Mine). Doesn't account for if the file does not exist.
# ...So mine will fail in airflow, but other than that it is written well. 
input_file = Path("/tmp/northstar/store_sales.csv")
def choose_path_1():
    if len(input_file.read_text().splitlines()) > 0: # Without '.read_text().splitlines()', just counts the characters in the file name. 
        return "build_report" 
    else:
        return "skip_report"

# Option 2 (Anyelle)
# ...Anyelle returns a pseudo row count of 0, so hers will work and take the skip_report path.  
SALES_FILE = Path("/tmp/northstar/store_sales.csv")
def count_rows():
    if not SALES_FILE.exists():
        return 0
    return len(SALES_FILE.read_text().splitlines())
def choose_path_2():
    row_count = count_rows()
    if row_count > 0:
        return "build_report"
    return "skip_report"

# Option 3 (Ironhack Answer)
def choose_path_3():
    report_required = True
    if report_required:
        return "build_report"
    return "skip_report"

"""
All 3 options essentially answer the question.
""" 

with DAG(
    dag_id = "q11",
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    # default_args = default_args,
    # max_active_runs = 1 ...No comment.
    tags = ["week7_day1", "airflow"]
) as dag:

    choose = BranchPythonOperator(task_id="choose", python_callable=choose_path_2)
 
    build_report = BashOperator(task_id="build_report", bash_command="echo 'building report'")
 
    skip_report = BashOperator(task_id="skip_report", bash_command="echo 'report skipped'")
 
    choose >> [build_report, skip_report]

    """
    The `choose` function returns `"skip_report"` as a string, and Airflow matches that string to the `task_id` of 
    the next task, not to the Python variable name. In `skip_report = BashOperator(task_id="skip_report", ...)`, 
    the variable `skip_report` (no quotes) is only used in the code, for example to set the dependencies with 
    `choose >> [build_report, skip_report]`, while the `task_id` `"skip_report"` (in quotes) is the name Airflow knows 
    the task by. When the DAG runs, Airflow runs `choose`, which calls the branch function and gets back `"skip_report"`. 
    Airflow then looks for the task connected after `choose` with that `task_id`, runs it, and marks the other task, 
    `build_report`, as skipped.
    """