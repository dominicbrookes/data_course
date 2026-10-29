from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

"""
It’s a placeholder. Q2 is teaching you the structure: how to define tasks and put them in order. The name describes the job the 
task would do in a real BritMart pipeline, and the echo (bash version of print) stands in for the real work.
"""

with DAG(
    dag_id="britmart_airflow_basics",
    start_date=datetime(2026, 10, 28),
    schedule=None,
# schedule=None means the DAG does not create normal scheduled runs. It can still be triggered manually.    
    catchup=False,
) as dag:
 
    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command="echo extracting orders",
    )
 
    validate_orders = BashOperator(
        task_id="validate_orders",
        bash_command="echo validating orders",
    )
 
    extract_orders >> validate_orders