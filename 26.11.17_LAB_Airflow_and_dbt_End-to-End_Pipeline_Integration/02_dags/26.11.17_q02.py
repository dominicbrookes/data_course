# Note: I'm using the work I already did on 26.11.14 to make all of the questions run without having to build eveyrthing from scratch. 
# So this '26.11.17_LAB_Airflow_and_dbt_End-to-End_Pipeline_Integration' folder has a copy of '26.11.14_britmart_dbt''s work. 

# I needed the "raw" schema in this project's DuckDB, so I copied over dev.duckdb from 26.11.14.
# To test the bash_command I first ran "dbt run --select +fct_daily_sales". The "+" in front
# builds the model plus everything upstream it depends on, so the plain Q2 command then works.

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
    dag_id = "26.11.17_q02",
    start_date = datetime(2026, 11, 16),
    schedule = None,
    catchup = False,
    #default_args = default_args
    #max_active_runs = 1,
    tags = ["26.11.17"]
) as dag:

    # run_dbt = BashOperator(task_id = "run_dbt", bash_command = f"cd {DBT_PROJECT} && dbt run --select fct_daily_sales")
    # The bash_command is the terminal command Airflow runs for this task, exactly as if
    # I typed it myself. "cd" moves into my dbt project folder, because dbt looks for
    # dbt_project.yml in the folder it runs from and Airflow otherwise starts in a random
    # temporary folder. "&&" means only run the next command if the cd worked, so dbt
    # never runs in the wrong place. "dbt run --select fct_daily_sales" then builds just
    # the fct_daily_sales model in DuckDB rather than the whole project. If dbt succeeds
    # the task goes green; if it fails the task goes red and the dbt error shows in the
    # task log. Airflow decides when the command runs and whether it worked, while dbt
    # does the actual transformation.

    # The correct final command to skip the above terminal faff should have been the below. The above exists beacuse of the wording of the lab question. 
    run_dbt = BashOperator(task_id = "run_dbt", bash_command = f"cd {DBT_PROJECT} && dbt run --select +fct_daily_sales")










