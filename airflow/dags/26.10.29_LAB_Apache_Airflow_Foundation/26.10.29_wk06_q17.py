from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
 
with DAG(
    dag_id="britmart_fan_out_fan_in",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
) as dag:
 
    start = BashOperator(
        task_id="start",
        bash_command="echo start",
    )
 
    orders = BashOperator(
        task_id="orders",
        bash_command="echo orders",
    )
 
    customers = BashOperator(
        task_id="customers",
        bash_command="echo customers",
    )
 
    products = BashOperator(
        task_id="products",
        bash_command="echo products",
    )
 
    report = BashOperator(
        task_id="report",
        bash_command="echo report",
    )
 
    start >> [orders, customers, products] >> report

# Ironhacks answer is correct but the format looks differnt (both mine and ironhacks are correct):
# start >> [orders, customers, products]
# [orders, customers, products] >> report