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
    dag_id = "26.11.17_q17",
    start_date = datetime(2026, 11, 16),
    schedule = "@daily",
    catchup = False,
    #default_args = default_args
    #max_active_runs = 1,
    tags = ["26.11.17"]
) as dag:

    check_source = BashOperator(task_id = "check_source", bash_command = ('echo "Source ready."'))

    run_sales_models = BashOperator(task_id = "run_sales_models", bash_command = (f"cd {DBT_PROJECT} && dbt run --select +fct_daily_sales"), retries=1, retry_delay=timedelta(minutes=2))

    test_sales_models = BashOperator(task_id = "test_sales_models", bash_command = (f"cd {DBT_PROJECT} && dbt test --select +fct_daily_sales"))

    publish_sales = BashOperator(task_id = "publish_sales", bash_command = ('echo "Sales published."'))

    check_source >> run_sales_models >> test_sales_models >> publish_sales

