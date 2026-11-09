"""
BrightCart Daily Sales Pipeline: Airflow DAG.

This file only describes ORCHESTRATION: when the pipeline runs, in what order, and what happens when a step
fails. All the pipeline logic lives in 02_scripts (bronze.py, silver.py, gold.py, platinum.py), so exactly the
same code runs by hand (01_optional_manual_run.py) and under Airflow.

    [wait_for_customers, wait_for_orders] >> bronze >> silver >> gold >> quality_check >> platinum >> completion_message

What the DAG does:
    D1       Makes the 02_scripts folder importable, then imports the five run functions.
    D2       Settings shared by every task: retries, owner, and which day to run (RUN_DATE).
    D3       The DAG itself: name, schedule, start date, catchup, the run_date box in the trigger form.
    D4       Two FileSensors, in parallel, wait for the day's customers.csv and orders.csv to arrive in 01_raw.
    D5       Five PythonOperator tasks, one per layer, each calling that layer's run function with the run date.
    D6       A BashOperator prints a completion message, only reached if every step before it succeeded.
    D7       The dependencies: the order the tasks run in.

Which day is processed?
    * Scheduled runs use the run's own date (ds_nodash, e.g. 20261105).
    * Manual runs can choose a day by typing it in the "run_date" box when triggering, e.g. 20261103.
      Leave it empty to use the run's own date.

Airflow features used (from my labs): PythonOperator, BashOperator, FileSensor, schedule="@daily",
catchup=False, default_args with retries, a per task retries override, fan-in dependencies, tags.
New in this DAG: sys.path.insert, op_kwargs, Jinja templates ({{ ... }}), Param, owner, description.
"""

# ===========================================================================
# Imports and Settings
# ===========================================================================

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

# ===========================================================================
# Task D1: Make 02_scripts importable, then import the five run functions.
# ===========================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "02_scripts"))

from bronze import run_bronze  # noqa: E402
from silver import run_silver  # noqa: E402
from gold import run_gold, run_quality_check  # noqa: E402
from platinum import run_platinum  # noqa: E402
# ---------- CODE EXPLANATION ----------
# This DAG file lives in 03_dags, but the pipeline functions live in 02_scripts. Python only looks for imports in
# certain folders (a list called sys.path), and 02_scripts isn't one of them, so "from bronze import run_bronze"
# would fail with ModuleNotFoundError. PROJECT_ROOT is worked out the same way as in config.py: Path(__file__) is
# this file, .resolve() makes it the full address and .parents[1] goes up two levels (03_dags, then the BrightCart
# project folder). sys.path.insert(0, ...) adds 02_scripts to the front of that list, so Python looks there first.
# str(...) turns the Path into text because sys.path holds text. After that the imports work, and bronze.py can in
# turn find config.py, because it's in the same folder. Importing doesn't run anything: each script's final run
# block is skipped when it's imported, thanks to if __name__ == "__main__":. The "# noqa: E402" comments tell code
# checkers that putting imports below other code is deliberate here (E402 is the "imports should be at the top"
# warning), because the path has to be added first.

# ===========================================================================
# Task D2: Settings shared by every task, and which day to run.
# ===========================================================================

DEFAULT_ARGS = {
    "owner": "dominic",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
# ---------- CODE EXPLANATION ----------
# default_args (wk07 q05) are settings every task in the DAG gets automatically, unless a task sets its own. One
# retry after a one minute wait helps with temporary problems, like a file still being written, but won't fix bad
# data or bugs. "owner" is new: it is just a label shown in the Airflow screen saying who looks after the DAG.

RUN_DATE = "{{ params.run_date if params.run_date else ds_nodash }}"
RAW_DIR = str(PROJECT_ROOT / "01_data" / "01_raw")
# ---------- CODE EXPLANATION ----------
# RUN_DATE is a Jinja template, which is new. Anything inside {{ }} is not Python: Airflow fills it in when each
# task actually runs, and the task receives the finished text. ds_nodash is a value Airflow provides for every run:
# the run's date as YYYYMMDD, e.g. 20261105, which matches my folder names. params.run_date is the date typed into
# the trigger form (see D3). So it reads: "use the typed date if there is one, otherwise the run's own date".
# Python just sees RUN_DATE as a normal piece of text until Airflow fills it in. RAW_DIR is the full address of
# 01_raw as text, so the sensors in D4 can build the path to the day's files.

# ===========================================================================
# Task D3: The DAG - name, schedule, start date, catchup, and the run_date box.
# ===========================================================================

with DAG(
    dag_id="brightcart_daily_sales_pipeline",
    description="BrightCart: land, clean, report, quality check and publish one day of sales data",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_date": Param(
            "",
            type="string",
            description="Day to process as YYYYMMDD, e.g. 20261105. Leave empty to use the run's own date.",
        ),
    },
    tags=["brightcart", "mini_project"],
) as dag:
    # ---------- CODE EXPLANATION ----------
    # schedule="@daily", start_date and catchup=False are the same as my labs (wk07 q01): run once a day at
    # midnight, and don't go back and run every missed day since the start date. description is new: a one line
    # summary shown in the Airflow screen. params is new: Param(...) adds a "run_date" box to the form you get when
    # you trigger the DAG by hand. "" means it's empty by default and type="string" means it holds text. This is
    # how a run can be pointed at 20261103, 20261104 or 20261105, the days I actually have data for, instead of
    # today's date, which has no files in 01_raw.

    # ===========================================================================
    # Task D4: Two FileSensors, in parallel, wait for the day's raw files.
    # ===========================================================================

    wait_for_customers = FileSensor(
        task_id="wait_for_customers",
        filepath=RAW_DIR + "/" + RUN_DATE + "/customers.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
        retries=0,
    )

    wait_for_orders = FileSensor(
        task_id="wait_for_orders",
        filepath=RAW_DIR + "/" + RUN_DATE + "/orders.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
        retries=0,
    )
    # ---------- CODE EXPLANATION ----------
    # The same FileSensor as wk07 q13 and q20: it checks for the file every 30 seconds (poke_interval) and gives up
    # after 10 minutes (timeout=600 seconds), and fs_default is the Connection that tells it to look on the machine
    # Airflow runs on. What's new is that filepath contains the RUN_DATE template, so Airflow fills in the day when
    # the sensor runs: e.g. .../01_raw/20261105/orders.csv. mode="reschedule" (from my q13 notes) frees up the
    # sensor's slot between checks instead of sitting there waiting. retries=0 overrides the default retry: if the
    # files never arrive within the timeout, waiting another 10 minutes won't help. For days that already have
    # files (20261103 to 20261105) the sensors pass straight away.

    # ===========================================================================
    # Task D5: One PythonOperator per layer, each calling that layer's run function.
    # ===========================================================================

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze,
        op_kwargs={"run_date": RUN_DATE},
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=run_silver,
        op_kwargs={"run_date": RUN_DATE},
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=run_gold,
        op_kwargs={"run_date": RUN_DATE},
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
        op_kwargs={"run_date": RUN_DATE},
        retries=0,
    )

    platinum = PythonOperator(
        task_id="platinum",
        python_callable=run_platinum,
        op_kwargs={"run_date": RUN_DATE},
    )
    # ---------- CODE EXPLANATION ----------
    # python_callable (wk07 q13) is the function the task runs, written without brackets so Airflow runs it later,
    # not now. op_kwargs is new: a dictionary of the arguments to hand to that function. {"run_date": RUN_DATE}
    # means Airflow calls run_bronze(run_date="20261105"), filling in the template first. It is the DAG's version of
    # sys.argv[1] in each script's final run block. quality_check has retries=0, the same as Anyelle's: a failed
    # check means the data is wrong, and checking the same data again gives the same answer. Every other task keeps
    # the one retry from DEFAULT_ARGS.

    # ===========================================================================
    # Task D6: Completion message, only reached if every step succeeded.
    # ===========================================================================

    completion_message = BashOperator(
        task_id="completion_message",
        bash_command=f'echo "BrightCart pipeline completed successfully for {RUN_DATE}"',
    )
    # ---------- CODE EXPLANATION ----------
    # A BashOperator echo, like my early labs, but the message includes the run date. The f-string puts the
    # RUN_DATE template text into the command, and Airflow then fills in the date when the task runs. It only runs
    # if every task before it succeeded, so seeing it green in Airflow means the whole day passed.

    # ===========================================================================
    # Task D7: The dependencies - the order the tasks run in.
    # ===========================================================================

    [wait_for_customers, wait_for_orders] >> bronze >> silver >> gold >> quality_check >> platinum >> completion_message
    # ---------- CODE EXPLANATION ----------
    # The square brackets are the fan-in pattern from wk06 q10: the two sensors don't depend on each other, so they
    # run at the same time, and bronze only starts once both have succeeded. After that it's a straight line, like
    # wk08 q17. If any task fails, every task after it is marked upstream_failed and never runs, so nothing is built
    # or published from bad data. The arrows only set the ORDER: the tasks share their results through files, not
    # by passing DataFrames to each other.