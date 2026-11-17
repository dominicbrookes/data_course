# Note: I'm using the work I already did on 26.11.14 to make all of the questions run without having to build eveyrthing from scratch. 
# So this '26.11.17_LAB_Airflow_and_dbt_End-to-End_Pipeline_Integration' folder has a copy of '26.11.14_britmart_dbt''s work. 

from airflow.sdk import DAG
from airflow.sdk import BaseHook, Variable

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor 

from datetime import datetime, timedelta
from pathlib import Path
# Imported all the libaraies I need for this lab, not sure thats best practise.

DBT_PROJECT = Path(__file__).parent.parent
# Works out the dbt project folder from where this DAG file lives. __file__ is this
# file, the first .parent is the 02_dags folder and the second .parent is the project
# folder. This means the command still works if the project folder is renamed or moved.
# The below python will allow this to work no matter how deep the file was...
# DBT_PROJECT = next(folder for folder in Path(__file__).parents if (folder / "dbt_project.yml").exists())

with DAG(
    dag_id = "26.11.17_q10",
    start_date = datetime(2026, 11, 16),
    schedule = None,
    catchup = False,
    #default_args = default_args
    #max_active_runs = 1,
    tags = ["26.11.17"]
) as dag:

    load_source = BashOperator(task_id = "load_source", bash_command = ('echo "Source ready."'))

    run_dbt = BashOperator(task_id = "run_dbt", bash_command = f"cd {DBT_PROJECT} && dbt run --select marts")

    test_dbt = BashOperator(task_id = "test_dbt", bash_command = f"cd {DBT_PROJECT} && dbt test --select marts")

    publish = BashOperator(task_id = "publish", bash_command = ('echo "Publish report."'))

    load_source >> run_dbt >> test_dbt >> publish

# I used Airflow's built-in "dags test" command to run the whole DAG once from the terminal, not just the dbt commands.
# It should end with "DagRun Finished: dag_id=26.11.17_q10 ... state=success".
# cd ~/data_course
# source .venv_airflow/bin/activate
# airflow dags test 26.11.17_q10
# I can also check the file has no errors with: python ~/data_course/26.11.17_LAB_Airflow_and_dbt*/02_dags/26.11.17_q10.py
# If nothing is returned, the file is fine.