# Remember that Option 2 needs the file, Variable and Connection created before it’ll run green.

"""
Part 4 — Sensors, Variables, Connections, and Alerts

Q. 13 — Wait for a File with a Sensor

Create a complete DAG containing a FileSensor.

Wait for: /tmp/britmart/orders.csv

Use: poke_interval = 30, timeout = 300

Add a second task named: process_file

that runs only after the file is available.
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

"""
# Option 1

with DAG(
    dag_id = "q_13",
    start_date = datetime(2026, 10, 30),
    schedule = None,
    catchup = False,
    tags=["week6_day3", "northstar"]
) as dag:

    sensor = FileSensor(
        task_id = "sensor",
        filepath = "/tmp/britmart/orders.csv",
        fs_conn_id = "fs_default",
        poke_interval = 30,
        timeout = 300
    )

    process_file = BashOperator(task_id = "process_file", bash_command = "echo 'Process File.'")

    sensor >> process_file


# Does the sensor check every 30 seconds for the file, and would a 24-hour timeout make it keep checking all day? 
# ...Yes: `poke_interval = 30` checks for the file every 30 seconds, and `timeout = 300` is the total time it will 
# ...wait (5 minutes). If the file appears, the sensor succeeds and `process_file` runs. If not, the sensor fails 
# ...at the timeout. A timeout of 86400 seconds (24 hours) would keep it checking every 30 seconds for up to a day. 
# ...For long waits, `mode = "reschedule"` frees up its slot between checks.

# 'fs_conn_id = "fs_default",' This line...Ubuntu is a second operating system running inside Windows, which I 
# ...installed in Step 1 of the setup guide using `wsl --install`. It keeps all its own files inside one big 
# ...file stored on the C drive, so Windows and Ubuntu are two separate file systems on the same laptop that 
# ...can each see the other. Windows starts at `C:`, while Ubuntu starts at `/`. From Ubuntu, the C drive 
# ...appears at `/mnt/c/`, and from Windows, Ubuntu's files appear at `//wsl$/Ubuntu-24.04/` (the Linux entry 
# ...in File Explorer). Airflow runs inside Ubuntu, so every path in a DAG is an Ubuntu path. That means the 
# ...sensor's `/tmp/britmart/orders.csv` is in Ubuntu's `tmp` folder, not on the C drive, and the `fs_default` 
# ...Connection, which I created at the end of Step 7 of the setup, just tells the sensor to check that exact 
# ...path on the machine Airflow runs on.

# ...Had to edit some of the file paths in the example above despite them being in tripple quotes.
"""    

# Option 2 (Anyelle's Version)

"""Week 6, Day 3 · Demo 4: Sensors, Variables and Connections (slides 33 to 36).

Before running, prepare (see README): the file /tmp/northstar/store_sales.csv,
the Variable report_region and the Connection northstar_warehouse.
"""
from datetime import datetime

from airflow.sdk import DAG
from airflow.sdk import BaseHook, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


def read_settings():
    # default="UK" is used only if the Variable was not created yet
    region = Variable.get("report_region")
    print(f"Report region: {region}")

"""
Variables: A Variable is a named value saved inside Airflow rather than in the DAG code, e.g. report_region = UK, and can be viewed or edited 
in the UI under Admin → Variables. In Anyelle’s DAG, the read_settings function reads it with Variable.get("report_region") and prints it to the 
log. The task runs that function because of python_callable=read_settings, while task_id is only the task’s name in the UI. The demo just prints 
the value, but in a real pipeline it would be used to control what the DAG does, such as filtering a report to one region. The point is that a 
setting can be changed in Airflow without editing the code. If the Variable hasn’t been created, the task fails, unless a default is given, 
e.g. Variable.get("report_region", default="UK").
"""


def show_connection():
    conn = BaseHook.get_connection("northstar_warehouse")
    print(f"Connection type: {conn.conn_type}")
    print(f"Host: {conn.host}")
    print(f"User: {conn.login}")
    print("Password: stored in Airflow, never printed and never in the code")
"""
Connections: A Connection stores the login details for a system outside Airflow, such as a database, cloud storage, an API or SFTP: its type, 
host, username and password. These are saved securely inside Airflow under Admin → Connections, so passwords are never written in the code, where 
they could end up on GitHub, and if a password changes it only needs updating once in Airflow. In the demo, show_connection reads 
one with BaseHook.get_connection("northstar_warehouse") and prints its type, host and username, but deliberately not the password. 
In a real pipeline, the Connection would be used to log in and run SQL, e.g. with SQLExecuteQueryOperator(conn_id="northstar_warehouse", ...). 
Not every DAG needs a Connection; only tasks that log into an outside system do, such as an extract task reading from a source database or a 
load task writing to a warehouse, while tasks like Bash commands or branching don’t. Most real pipelines use at least one. 
The fs_default Connection used by FileSensor is also a Connection, telling the sensor to check file paths on the machine Airflow runs on.
"""


with DAG(
    dag_id="03_sensor_variable_connection",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["week6_day3", "northstar"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/northstar/store_sales.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
    )

    settings = PythonOperator(task_id="read_settings", python_callable=read_settings)
    connection = PythonOperator(task_id="show_connection", python_callable=show_connection)

    # The slide also shows SQLExecuteQueryOperator(conn_id="northstar_warehouse", ...).
    # It needs a real database, so this demo reads the Connection details instead.

    wait_for_file >> settings >> connection