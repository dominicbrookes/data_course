# BrightCart Pipeline: Recovery Plan

What to do when a run fails or stops part way.

The DAG `brightcart_daily_sales_pipeline` runs these tasks in order:

```
[wait_for_customers, wait_for_orders] >> bronze >> silver >> gold >> quality_check >> platinum >> completion_message
```

All commands below are run from `~/data_course` with the venv active (`source .venv_airflow/bin/activate`).

---

## The golden rule

**Fix the cause first, then rerun.** Clearing or rerunning a task before fixing the cause only repeats the same failure.

Every recovery follows the same order:

1. Find the **first** task that failed, not the last red one. Everything after it will show `upstream_failed`.
2. Open its **Logs** and find the traceback. The last lines name the error, and the lines above it name the function, which maps to a step number (e.g. `customer_validation` = S4 in `silver.py`).
3. Decide: is the cause **temporary** (it will fix itself, e.g. a late file) or **deterministic** (it needs a fix, e.g. bad data or a code bug)?
4. Fix or restore the cause, and check the fix.
5. Rerun the day: clear the failed task with its downstream tasks, trigger a new run with `run_date`, or use the manual run script.
6. Check that every task is green and that `01_data/06_audit/<run_date>.json` shows `"status": "success"`.

---

## Why reruns are safe

The pipeline is **idempotent**: every layer replaces that day's files rather than adding to them, and the SQLite load deletes that day's rows before appending the new ones. Running the same day twice gives the same files and the same totals (see section 8 of the README).

The raw files in `01_data/01_raw/` are never modified, so any day can always be rebuilt from them, all the way from bronze.

---

## Where to look

| What you see | Most likely cause | Where to look |
|---|---|---|
| DAG doesn't appear in the UI | Syntax error or failed import in the DAG file, or Airflow is reading a different DAGs folder | Import errors banner in the UI; `dags_folder` in `airflow.cfg` must be `/home/dominicbrookes/data_course`; check `.airflowignore` isn't hiding the project folder |
| `ModuleNotFoundError: No module named 'bronze'` (or `silver`, `gold`, `platinum`) | The DAG can't find `02_scripts` (folder moved or renamed) | The `sys.path.insert` line in the DAG: it must point at `PROJECT_ROOT / "02_scripts"` |
| `ModuleNotFoundError: No module named 'sweetviz'` | Sweetviz not installed in the venv | `pip install sweetviz` in `.venv_airflow` |
| `wait_for_customers` / `wait_for_orders` failed | The file didn't arrive within 10 minutes (sensor timeout) | The sensor's log shows the path it was poking; check `01_data/01_raw/<run_date>/` |
| `bronze` failed | A file is missing or empty, or source rows ≠ bronze rows | Its log: B1 names the missing or empty file; B5 names the file and both counts |
| `silver` failed | Unexpected file content (missing column, a value that breaks a step), counts don't reconcile, the join multiplied rows, or the SQLite load failed | Its log: the traceback names the step; S12 prints both counts; a `MergeError` comes from the S11 `many_to_one` join; a column error in S14 means the file's columns changed |
| `gold` failed | Silver valid ≠ completed + not completed (G6), or no completed orders for the day (G5 `idxmax` fails on an empty table) | Its log: G6 prints both counts; an `argmax of an empty sequence` error comes from G5 |
| `quality_check` failed | The outputs disagree, a required file is missing, or the reports are stale | Its log: every check is printed as `PASS` or `FAIL`; the audit shows the same results |
| `platinum` failed | No cleaned orders file for the day, or a Sweetviz problem | Its log: P1 says to run the pipeline for that day first |
| `database is locked` in the silver log | DBeaver is holding `brightcart.db` open with an unfinished change | Close or commit in DBeaver, then rerun silver |
| A task shows `upstream_failed` | An earlier task failed | Go back up the chain to the first failed task |
| A task shows `up_for_retry` | It failed once and Airflow will try again in 1 minute (bronze, silver, gold, platinum) | Wait for the retry before acting |
| Findfont "Roboto" warnings in the platinum log | Sweetviz looking for a font | Harmless; the report is still saved |

---

## Scenario 1: the source files didn't arrive (real example)

This happened on **20261109**. The DAG was left switched on, so Airflow started its scheduled daily run, but no files had been put in `01_data/01_raw/20261109/`. Evidence: `05_evidence/20261109_sensor_fail.png`.

**What you see**

* `wait_for_customers` and `wait_for_orders` stay running (yellow) while they check for the files every 30 seconds, then go to `failed` after 10 minutes (`timeout=600`). They have `retries=0`, so there's no second attempt.
* `bronze`, `silver`, `gold`, `quality_check`, `platinum` and `completion_message` all show `upstream_failed`. None of them ran, so no outputs or audit were written for that day.
* The sensor log shows it poking for `.../01_data/01_raw/20261109/customers.csv` (or `orders.csv`) until it times out.

**Temporary or deterministic?** Temporary if the files are only late. Deterministic if they are missing, misnamed or in the wrong folder. Here there was no data for that day at all, so the right fix was to pause the DAG, not to rerun it.

**Recovery**

1. Check the folder in the sensor log. Is the date folder in `YYYYMMDD` format? Are the files named exactly `customers.csv` and `orders.csv`?
2. Get the files from the source and place them in `01_data/01_raw/<run_date>/`. Never create or edit the data by hand.
3. Rerun the day, either:
   * in the Airflow UI, open the failed run, select `wait_for_customers` and `wait_for_orders`, and click **Clear** with **downstream** tasks; or
   * click **Trigger** and type the date into the `run_date` field.
4. Confirm all 8 tasks are green and the audit shows `"status": "success"`.

**Prevention:** pause the DAG (the toggle next to its name) when no new files are expected, so it doesn't create a failed run every night.

---

## Scenario 2: bad data breaks a step

**What you see**

* For example, `bronze` is green but `silver` is red, and everything after it is `upstream_failed`.
* The log ends with a traceback naming the function, e.g. `customer_validation` (S4) or `join_orders_customers` (S11).
* Typical causes: a column renamed or missing in the new file, a column added (the SQLite load in S14 fails because the table's columns no longer match), or counts that no longer reconcile (S12 stops the run).

**Recovery**

1. Read the traceback and match the function to its step number in the script.
2. Look at the day's bronze file (`01_data/02_bronze/<run_date>/`) to see what is different about the data.
3. Decide:
   * The **file** is wrong (e.g. a broken export): get a corrected file from the source and replace it in `01_raw`.
   * The **code** needs to handle a new case: fix the step, then test it on its own: `python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/silver.py <run_date>`
   * New columns in SQLite: decide whether the table should change. If so, drop that table in DBeaver so S14 can recreate it (every day must then be rerun to refill it).
4. Rerun the day from bronze (see Scenario 4), so every layer is rebuilt from the raw files.

---

## Scenario 3: the quality gate failed

**What you see**

* The sensors, `bronze`, `silver` and `gold` are green.
* `quality_check` is `failed`. It has `retries=0` on purpose: retrying bad data gives the same result.
* `platinum` and `completion_message` are `upstream_failed`, so the day is **not** reported as complete.
* The log shows lines such as `FAIL  revenue: city report = daily summary`.
* The audit file shows `"status": "failed"` and the result of every check under `quality_checks`.

**Recovery**

1. Read which check failed:
   * **counts don't reconcile:** rows were lost or created. Look at the validation steps in `silver.py` (S4, S10) and the count check (S12).
   * **revenue doesn't match:** a gold or audit file was edited by hand, or the report code changed (G1, G4, G5, G8). Never edit outputs by hand.
   * **duplicate order_id or missing values in valid orders:** a validation rule in S10 isn't working.
   * **reports aren't fresh:** the gold reports on disk are from an earlier run (older than 60 minutes), so `gold` didn't really write new files for this run.
2. Fix the code or rebuild the files from the pipeline, never by hand.
3. Test manually first: `python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/gold.py <run_date>` runs `run_gold` then `run_quality_check`.
4. Rerun the day from bronze (see Scenario 4), so every output is rebuilt from the raw files.

---

## Scenario 4: the run stopped part way

**What you see**

* For example, `bronze` and `silver` succeeded but `gold` failed (a crash, the machine restarted, Airflow was stopped mid-run).
* `01_data/03_silver/.../<run_date>/` has new files, but `01_data/04_gold/<run_date>/` is missing or holds files from an earlier run.
* The audit is missing, or still shows `"status": "pending_quality_check"`.

Nothing is duplicated, because every layer replaces its files, but the layers now come from different runs.

**Recovery**

1. Read the log of the failed task and fix the cause.
2. **Rerun the whole day from bronze.** This is the safest option, because every layer is rebuilt from the raw files in order, and there's no need to work out which files can be trusted. There's no need to delete the partial files first: they are replaced. Either:
   * in the Airflow UI, select `bronze` in the failed run and click **Clear** with **downstream** tasks (the sensors already passed, so they don't need to run again); or
   * click **Trigger** with the `run_date`; or
   * run it without Airflow: `python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py <run_date>`
3. The day is only finished when `quality_check` is green and the audit shows `"status": "success"`.

---

## Scenario 5: several days were missed (backfill)

**What you see**

The DAG was paused or broken for several days, for example 20261103 to 20261105. Because the DAG has `catchup=False`, Airflow does **not** run the missed days by itself when it is switched back on.

**Recovery**

1. Check the raw files exist for every missing day in `01_data/01_raw/`.
2. Check which days already have an audit in `01_data/06_audit/`.
3. Process the days **in order**, oldest first, either:
   * in the UI, **Trigger** with `run_date` = `20261103`, then `20261104`, then `20261105`; or
   * manually, in one command: `python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py 20261103 20261104 20261105` (with no dates, it runs every day found in `01_raw`). If one day fails, the days after it aren't run: fix it and run again.
4. Validate each day: its audit must show `"status": "success"`.
5. Because outputs are replaced, reprocessing a day that already existed doesn't duplicate anything, including in SQLite.

---

## Retry, clear, new run or manual run?

| Action | What it does | Use it when |
|---|---|---|
| **Retry** | Airflow tries the same task again automatically (1 retry, after 1 minute; none for the sensors or the quality gate) | The cause may be temporary |
| **Clear** (with downstream) | Resets the selected task and everything after it inside the existing run | You've fixed the cause and want to finish the same run |
| **Trigger** with `run_date` | Creates a new, separate run for that day | You want to process a specific day, or test after a change |
| **Manual run script** | Runs bronze → silver → gold → quality gate → platinum in the terminal, without Airflow | Testing a fix quickly, or Airflow isn't running |

---

## Daily checklist

A green DAG only proves the code finished. For each day, also check:

* [ ] The DAG run is `success`, and no task is `failed` or `upstream_failed`.
* [ ] Both source files exist in `01_data/01_raw/<run_date>/`.
* [ ] Outputs exist in `03_silver/01_valid/`, `03_silver/02_rejected/`, `04_gold/` and `05_platinum/` for the day.
* [ ] The audit `01_data/06_audit/<run_date>.json` shows `"status": "success"`, both `"reconciles": true`, and every quality check `true` (including freshness).
* [ ] The rejected count is in the usual range (13 to 17 orders, 2 customers so far). A sudden jump means a problem in the source system.
* [ ] Completed revenue looks plausible compared with previous days.