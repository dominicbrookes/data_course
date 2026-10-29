from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = "q10",
    start_date = datetime(2026, 10, 28),
    schedule = None,
    catchup = False,
) as dag:

    extract_orders = BashOperator(
        task_id = "extract_orders",
        bash_command = "echo 'Extract orders.'")

    extract_customers = BashOperator(
        task_id = "extract_customers",
        bash_command = "echo 'Extract customers.'")

    build_report = BashOperator(
        task_id = "build_report",
        bash_command = "echo 'Build report.'")

    [extract_orders, extract_customers] >> build_report

    """
    Putting extract_orders and extract_customers in a list ([extract_orders, extract_customers] >> build_report) means the 
    two extract tasks don’t depend on each other, so they can run at the same time, and build_report only starts once both 
    have succeeded. If I’d written extract_orders >> extract_customers >> build_report instead, build_report would still wait 
    for both, but extract_customers would also have to wait for extract_orders even though it doesn’t need it. That makes the 
    pipeline slower, and if extract_orders failed, extract_customers wouldn’t run at all.
    """