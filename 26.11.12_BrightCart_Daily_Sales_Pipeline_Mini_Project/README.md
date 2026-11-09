# BrightCart Daily Sales Pipeline

An Ironhack data engineering mini project (Weeks 7–8, November 2026) by Dominic Brookes.

BrightCart is a small UK online retailer. Every day two files arrive: `customers.csv` and `orders.csv`. The files are messy (status written in several ways, £ signs in prices, spaces in city names, two date formats, missing values, duplicate orders, unknown customers). This project builds a daily batch pipeline that:

- keeps the raw files unchanged
- validates customers and orders, fixing what can be fixed safely and rejecting what can't
- saves valid and rejected rows separately, with a `rejection_reason` on every rejected row
- builds a daily sales report (completed revenue by city, plus a one-row daily summary)
- writes an audit record proving that source = valid + rejected
- runs a quality gate before a day counts as a success
- is orchestrated by an Airflow DAG with clear task boundaries

**The rule the pipeline implements:** an order counts as revenue only when its status is `COMPLETED`, its quantity and price are valid, and duplicates have been removed. Fix when the correction is certain; reject when the value is unknown.

---

## 1. Architecture

The pipeline uses a medallion architecture. Each layer reads only what the layer before it saved, and checks its own row counts before it writes anything.

```
raw  ──>  bronze  ──>  silver  ──>  gold  ──>  quality gate  ──>  platinum
```

| Layer | What it holds | Script |
|---|---|---|
| Raw | The files exactly as received. Never edited. | (input) |
| Bronze | A checked, unchanged copy of raw. Source rows = bronze rows. | `bronze.py` |
| Silver | Cleaned and validated data. Valid customers, valid orders joined to customers, and rejected rows with a reason. Also loaded into SQLite. | `silver.py` |
| Gold | The reports (city report, daily summary) and the audit record. | `gold.py` |
| Quality gate | Checks counts and revenue agree across silver, gold and the audit, and that the reports are fresh. Sets the audit status to `success` or `failed`. | `gold.py` (QC1–QC4) |
| Platinum | An automatic Sweetviz report comparing raw orders with cleaned orders. | `platinum.py` |

Checks run at every layer (bronze B5, silver S12, gold G6, then the quality gate), so a problem stops the run at the step that caused it rather than at the end ("fail fast").

### Airflow DAG

```
[wait_for_customers, wait_for_orders] >> bronze >> silver >> gold >> quality_check >> platinum >> completion_message
```

- Two `FileSensor` tasks wait for both input files for the run date before anything runs (fan-in).
- Five `PythonOperator` tasks each call one layer's `run_` function with the run date.
- `quality_check` has `retries=0`: a failed check is a data problem, so retrying won't fix it.
- A `BashOperator` prints a completion message with the date.
- Schedule `@daily`, `catchup=False`. A `run_date` parameter in the trigger form lets you run any day by hand; if it is left blank, Airflow's own date (`ds_nodash`) is used.

---

## 2. Folder layout

```
26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/
    00_notebooks/                 lab notebooks and lab DAG files (ignored by Airflow)
    01_data/
        01_raw/<date>/            customers.csv, orders.csv (as received)
        02_bronze/<date>/         exact copy of raw
        03_silver/01_valid/<date>/      customers.csv, orders.csv (orders joined to customers)
        03_silver/02_rejected/<date>/   customers.csv, orders.csv (with rejection_reason)
        04_gold/<date>/           city_report.csv, daily_summary_report.csv
        05_platinum/<date>/       orders_raw_vs_cleaned.html
        06_audit/<date>.json      audit record for the day
        07_sql_database/          brightcart.db (SQLite, all days; not on GitHub)
    02_scripts/
        config.py                 shared paths (get_paths)
        bronze.py  silver.py  gold.py  platinum.py
        01_optional_manual_run.py run the whole pipeline without Airflow
    03_dags/
        brightcart_daily_sales_pipeline.py
    04_other/
        init_notes.py             reference note: what __init__.py does and how this project could use it
        utils_notes.py            reference note: what utils.py does and how this project could use it
    05_evidence/                  Airflow screenshots
    README.md
    RECOVERY.md
```

The data folders map onto the course's raw / cleaned / curated / audit structure like this:

| Course folder | This project |
|---|---|
| `data/raw/` | `01_data/01_raw/` (plus `02_bronze/`, the checked copy) |
| `data/cleaned/` | `01_data/03_silver/01_valid/` and `01_data/03_silver/02_rejected/` |
| `data/curated/` | `01_data/04_gold/` (plus `05_platinum/`, the data quality report) |
| `data/audit/` | `01_data/06_audit/` |
| `notebooks/` | `00_notebooks/` |
| `scripts/` | `02_scripts/` |
| `dags/` | `03_dags/` |

The data folders are numbered in the same order as the layers, so the layout mirrors the architecture. Every layer saves one folder per day (partitioned by date), and the gold reports carry a `run_date` column so all days can be stacked into one table.

---

## 3. Setup

Everything is run from the `data_course` folder in WSL (Ubuntu 24.04), with the virtual environment active. Run the commands in the VS Code terminal, one line at a time.

> **Windows:** first check that the bottom left corner of VS Code shows **WSL: Ubuntu 24.04**. If it doesn't, click the blue `><` button and choose **Connect to WSL**.

```bash
cd ~/data_course
source .venv_airflow/bin/activate
pip install pandas numpy sweetviz ipykernel    # Airflow 3 is already installed in this venv
```

The terminal line should now start with `(.venv_airflow)`. To open the notebooks in `00_notebooks/`, choose **Select Kernel → Python Environments → .venv_airflow** at the top right of the notebook.

### Airflow

- Airflow home is `data_course/airflow`.
- In `airflow.cfg`, `dags_folder` points at the whole `data_course` folder, so Airflow can find the project's DAG in `03_dags/` without copying it anywhere.
- `data_course/.airflowignore` stops Airflow scanning folders it doesn't need:

  ```
  .venv_airflow
  .vscode
  01_notebooks
  logs
  00_notebooks
  ```

- Airflow only parses `.py` files that contain both the words "airflow" and "dag", and every script's run block sits under `if __name__ == "__main__":`, so importing a script never runs it.

> **GitHub:** Git doesn't save empty folders. If a folder is empty when you push, add an empty `.gitkeep` file inside it so the structure is kept. `brightcart.db` is not pushed (`*.db` is in `.gitignore`); it is rebuilt by running the pipeline.

Start Airflow:

```bash
airflow standalone
```

Then open http://localhost:8080 and log in as `admin` (the password is in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`). Stop it with Ctrl+C.

---

## 4. How to run it

### Option A: Airflow

1. Make sure the day's files are in `01_data/01_raw/<date>/`.
2. In the Airflow UI, open `brightcart_daily_sales_pipeline` and click **Trigger**.
3. Type the date (e.g. `20261105`) into the `run_date` field and trigger.
4. All 8 tasks should go green, and the `completion_message` log shows the date.

### Option B: without Airflow

`01_optional_manual_run.py` calls the same `run_` functions Airflow calls, in the same order.

```bash
# every day found in 01_raw, oldest first
python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py

# one day
python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py 20261105
```

Each layer script can also be run on its own for one day, e.g.:

```bash
python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/silver.py 20261105
```

### Run order

```
bronze  >>  silver  >>  gold  >>  quality gate  >>  platinum
```

If any step raises an error, the steps after it don't run, so later layers never work on bad data. Errors are raised rather than printed and swallowed, so Airflow marks the task as failed.

---

## 5. Validation rules

### Fixed (the correction is certain)

| Problem | Fix | Step |
|---|---|---|
| Spaces around values | Stripped from every text column | S1, S5 |
| City written in different cases | Title case | S2 |
| Status written in different cases | Upper case | S6 |
| £ sign in price | Removed | S7 |
| IDs, quantity and price stored as text | Converted to numbers; anything that can't convert (e.g. `"two"`) becomes missing and is rejected | S3, S8 |
| Two date formats (`2026-11-05` and `05/11/2026`) | ISO tried first, then UK format | S9 |

### Rejected (the value is unknown or can't be trusted)

**Customers (S4)**

| rejection_reason | Rule |
|---|---|
| `missing_customer_id` | No customer_id |
| `missing_customer_name` | No customer_name |
| `missing_city` | No city |
| `duplicate_customer_id` | customer_id already seen (first one kept) |

**Orders (S10)**, checked in this order:

| rejection_reason | Rule |
|---|---|
| `missing_value` | Missing order_id, customer_id, price, quantity or order date |
| `duplicate_order_id` | order_id already seen (first one kept) |
| `zero_or_negative_price_or_quantity` | Price or quantity ≤ 0 |
| `invalid_status` | Status not in `COMPLETED`, `CANCELLED`, `PENDING`, `REFUNDED` (e.g. `SHIPPD`) |
| `unknown_customer` | customer_id not in the valid customers |

Valid orders are then joined to valid customers (S11) with `validate="many_to_one"`, so a join can never silently multiply rows.

**Nothing is dropped silently.** Every source row ends up in either the valid file or the rejected file, never neither. Each rejected row keeps its values and a `rejection_reason`, and the count checks (silver S12 and the audit) prove that source = valid + rejected. If a row went missing, the run would stop.

### Rows rejected

| Day | Customers rejected | Orders rejected |
|---|---|---|
| 20261103 | 2 | 17 |
| 20261104 | 2 | 13 |
| 20261105 | 2 | 13 |

Examples from 20261103: customer 37 was rejected as `missing_customer_name`, and a second row for customer 12 (Ruby Turner) was rejected as `duplicate_customer_id` (the first one was kept). No customer was rejected for `missing_city` on any day.

The full breakdown by reason for each day is in that day's audit file (`06_audit/<date>.json`, under `rejected_by_reason`), and the rejected rows themselves are in `03_silver/02_rejected/<date>/`.

---

## 6. Outputs

| Output | Location | Contents |
|---|---|---|
| Valid customers | `03_silver/01_valid/<date>/customers.csv` | Cleaned customers |
| Valid orders | `03_silver/01_valid/<date>/orders.csv` | Cleaned orders joined to customer_name and city |
| Rejected customers / orders | `03_silver/02_rejected/<date>/` | Rejected rows with `rejection_reason` |
| City report | `04_gold/<date>/city_report.csv` | `run_date`, `city`, `completed_revenue_gbp`, `completed_orders`, highest revenue first |
| Daily summary | `04_gold/<date>/daily_summary_report.csv` | `run_date`, `completed_orders`, `completed_revenue_gbp`, `top_order_id`, `top_order_revenue_gbp`, `rejected_orders` |
| Audit | `06_audit/<date>.json` | Source, valid and rejected counts, rejections by reason, reconciliation, revenue, quality check results, report age, status |
| Data quality report | `05_platinum/<date>/orders_raw_vs_cleaned.html` | Sweetviz comparison of raw and cleaned orders |
| SQLite database | `07_sql_database/brightcart.db` | The four silver tables for every day, with a `run_date` column |

### Sample outputs (20261105)

- [Valid orders](01_data/03_silver/01_valid/20261105/orders.csv)
- [Rejected orders](01_data/03_silver/02_rejected/20261105/orders.csv)
- [City report](01_data/04_gold/20261105/city_report.csv)
- [Daily summary](01_data/04_gold/20261105/daily_summary_report.csv)
- [Audit](01_data/06_audit/20261105.json)
- [Data quality report](01_data/05_platinum/20261105/orders_raw_vs_cleaned.html) (download and open in a browser)

### Results

| Day | Bronze (customers / orders) | Customers valid + rejected | Orders valid + rejected | Completed + not completed | Quality gate |
|---|---|---|---|---|---|
| 20261103 | 41 / 58 | 39 + 2 | 41 + 17 | 32 + 9 | PASS, `success` |
| 20261104 | 44 / 63 | 42 + 2 | 50 + 13 | 36 + 14 | PASS, `success` |
| 20261105 | 46 / 61 | 44 + 2 | 48 + 13 | 39 + 9 | PASS, `success` |

Every day reconciles: source = valid + rejected for both customers and orders.

---

## 7. Audit and quality gate

Gold writes the audit with status `pending_quality_check`. The quality gate (its own Airflow task) then reads the files back from disk and runs these checks:

1. Customer counts reconcile (source = valid + rejected)
2. Order counts reconcile
3. No duplicate `order_id` in valid orders
4. No missing quantity or price in valid orders
5. Revenue: silver = city report
6. Revenue: city report = daily summary
7. Revenue: daily summary = audit
8. Freshness: both gold reports were written in the last 60 minutes

The results and report ages are added to the audit, and the status becomes `success` only if every check passes. The audit is saved even when a check fails, so the evidence is kept, and then the task raises an error so Airflow marks it failed.

### How to verify a run

1. **Airflow:** every task for the run is green (or the terminal ends with `(DB) Finished running.` for a manual run).
2. **Audit:** open `06_audit/<date>.json` and check:
   - `"status": "success"`
   - `"reconciles": true` under both `customers` and `orders`
   - every entry under `quality_checks` is `true`
3. **Counts by hand:** source rows in `01_raw/<date>/` = rows in `03_silver/01_valid/<date>/` + rows in `03_silver/02_rejected/<date>/`, for both files.
4. **Rejected rows:** open `03_silver/02_rejected/<date>/` and check each row's `rejection_reason` matches the rules in section 5.
5. **Revenue:** the total of `completed_revenue_gbp` in the city report matches the daily summary and the audit.
6. **SQL (optional, in DBeaver):**

   ```sql
   -- valid orders per day (expected: 41, 50, 48)
   SELECT run_date, COUNT(*) AS valid_orders
   FROM silver_orders_valid
   GROUP BY run_date;

   -- rejected orders by reason per day
   SELECT run_date, rejection_reason, COUNT(*) AS rejected_orders
   FROM silver_orders_rejected
   GROUP BY run_date, rejection_reason
   ORDER BY run_date, rejected_orders DESC;
   ```

---

## 8. Re-running a day (idempotency)

Each day can be re-run as many times as needed without duplicating data or audits.
Running the same day once or five times gives the same result.

This comes from two things working together:

1. **One folder per day.** `get_paths(run_date)` in `config.py` puts the run date in every path,
   so each day's files are kept separate. Re-running one day never touches another day.
2. **Files are replaced, not appended.** Every save uses `.to_csv()` or `.write_text()`, which
   overwrite an existing file by default. No CSV or audit file is ever appended to. The SQLite
   database is the one place rows are appended, and it deletes that day's rows first (see S14 below).

| File | Step | What it does on a re-run |
|---|---|---|
| `config.py` | `get_paths` | Puts `run_date` in every path, so each day has its own folders and files |
| `bronze.py` | B3 | `.mkdir(exist_ok=True)` carries on if the day folder exists; `.to_csv()` replaces the bronze file |
| `silver.py` | S13 | `.mkdir(exist_ok=True)` x2; `.to_csv()` x4 replaces the valid and rejected files |
| `silver.py` | S14 | Deletes that day's rows from each SQLite table, then appends the new ones (like `DELETE WHERE run_date = ...` then `INSERT`) |
| `gold.py` | G7 | `.mkdir(exist_ok=True)`; `.to_csv()` x2 replaces the city report and daily summary |
| `gold.py` | G9 | `.mkdir(exist_ok=True)`; `.write_text()` replaces the audit file |

After a re-run, the only changes in the audit are `run_at_utc`, which shows the latest run time, and
`output_age_minutes`, which shows how old the reports were when the quality gate checked them.

**Known limitation:** if a re-run fails part way (for example bronze and silver succeed but gold fails),
the earlier layers hold new files while the later layers still hold files from the previous run.
Nothing is duplicated, but the layers come from different runs. The quality gate's freshness check
(QC2 in `gold.py`) catches this, and the fix is to re-run the whole day from bronze (see `RECOVERY.md`).

---

## 9. Viewing the outputs

**DBeaver (SQLite):** New connection → SQLite → path:

```
\\wsl$\Ubuntu-24.04\home\dominicbrookes\data_course\26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project\01_data\07_sql_database\brightcart.db
```

**Power BI:** Get Data → Folder → point at `04_gold` via `\\wsl.localhost\Ubuntu-24.04\home\dominicbrookes\data_course\...`, filter to `city_report.csv`, then Combine Files. Convert `run_date` (YYYYMMDD text) to a date in Power Query. WSL must be running.

---

## 10. Recovery

See [RECOVERY.md](RECOVERY.md) for what to do when a run fails, including a real example: a scheduled run on 20261109 where no files had arrived, so both sensors timed out and every task after them was marked `upstream_failed`.

---

## 11. Evidence

In `05_evidence/`:

- `20261103_success.png`, `20261104_success.png`, `20261105_success.png`: all 8 tasks green for each day, triggered with the run date
- `20261109_sensor_fail.png`: the failed scheduled run used in RECOVERY.md

---

## 12. Known limitations

- **Grouped order rejection reasons:** missing values share one reason (`missing_value`), and zero/negative price and quantity share another. Separate reasons would make the rejected file more precise.
- **One reason per row:** rules run in order, so a row that breaks two rules only shows the first.
- **Rejected files show cleaned values:** e.g. a quantity of `"two"` appears blank in the rejected file. The original value is still in bronze.
- **No completed orders:** the daily summary uses `idxmax()` to find the top order, which would crash on a day with zero completed orders.
- **New columns:** if a future file arrives with an extra column, the SQLite append would fail because the table's columns no longer match.
- **Freshness check:** it mainly catches unusual cases, such as re-running only the quality gate on old reports.
- **Error location:** Airflow shows which task failed (e.g. `silver`), but you need the task log's traceback to see which step (e.g. `customer_validation`, S4).
- **Scripts aren't a package:** the DAG finds `02_scripts` with `sys.path.insert` rather than importing it as a package with an `__init__.py`. It works, but VS Code can't follow it, so the DAG file shows import warnings. A package would also need the folder renaming, because Python names can't start with a digit. See `04_other/init_notes.py`.
- **Some steps repeat across layers:** saving a CSV, counting rows by reading a file back, and the final run block are written out in each script. They could be shared helpers in a `utils.py`. Each layer was kept self-contained so it reads top to bottom. See `04_other/utils_notes.py`.
- **Settings live in the code:** settings such as `FRESHNESS_MAX_AGE_MINUTES` and `VALID_STATUSES` sit inside the scripts. In a larger project they would move into a config file or Airflow Variables, so they could change without editing code.