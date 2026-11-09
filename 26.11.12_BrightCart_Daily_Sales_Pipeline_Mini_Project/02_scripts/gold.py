"""
BrightCart Daily Sales Pipeline: gold layer.

Builds the daily sales reports from silver, and writes the audit record for the run.
    Reads:   01_data/02_bronze/<run_date>/customers.csv, orders.csv (row counts for the audit only)
             01_data/03_silver/01_valid/<run_date>/customers.csv, orders.csv
             01_data/03_silver/02_rejected/<run_date>/customers.csv, orders.csv
    Writes:  01_data/04_gold/<run_date>/city_report.csv, daily_summary_report.csv
             01_data/06_audit/<run_date>.json

What gold does:
    G1-G3    Adds line_revenue_gbp (quantity x unit price) and run_date, then splits orders into
             completed and not completed. Only COMPLETED orders count as revenue.
    G4       City report: completed orders and revenue per city, highest revenue first.
    G5       Daily summary report: completed orders, completed revenue, top order, rejected orders.
    G6       Checks silver valid orders = completed + not completed. Stops the run if not.
    G7       Writes the two reports (only if G6 passed).
    G8-G9    Builds and writes the audit: source, valid and rejected counts for customers and orders,
             rejections by reason, whether they reconcile, revenue, run time, status.
             Status is "pending_quality_check" until the quality gate (QC1-QC4) checks it.
    G10      run_gold: runs G1 to G9 in order. This is what Airflow calls.

Quality gate (its own Airflow task, runs after run_gold):
    QC1      Quality checks: counts reconcile, no duplicate order_id, no missing quantity or price,
             revenue matches across silver, city report, daily summary and audit.
    QC2      Freshness check: the gold reports were written in the last 60 minutes.
    QC3      Updates the audit with the check results and sets status to success or failed.
    QC4      run_quality_check: checks the files it needs exist, runs QC1 to QC3, stops the run if anything
             failed. This is what Airflow calls.

Final run:
    G11      Runs run_gold then run_quality_check when the file is started from the terminal (skipped when Airflow imports it).

Run it on its own from the data_course folder (runs run_gold then run_quality_check):
    python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/gold.py 20261105
"""

# ===========================================================================
# Imports and Settings
# ===========================================================================

from datetime import datetime, timezone
import json
import sys
import pandas as pd

from config import get_paths

# ===========================================================================
# Configuration
# ===========================================================================

# N/A

# ===========================================================================
# Task G1: Add revenue column.  
# ===========================================================================

def add_revenue(silver_valid):

    gold_add_revenue = silver_valid.copy()

    gold_add_revenue["line_revenue_gbp"] = (gold_add_revenue["quantity"] * gold_add_revenue["unit_price_gbp"]).round(2)
    return gold_add_revenue

# ===========================================================================
# Task G2: Add run_date column.
# ===========================================================================

def add_run_date(gold_add_revenue, run_date):

    gold_add_run_date = gold_add_revenue.copy()

    gold_add_run_date["run_date"] = run_date
    return gold_add_run_date

# ===========================================================================
# Task G3: Only work on orders where the status == COMPLETED.
# ===========================================================================

def only_complete(gold_add_run_date):

    gold_only_complete = gold_add_run_date.copy()

    gold_only_complete_mask = gold_only_complete["status"] == "COMPLETED"
    gold_status_completed = gold_only_complete[gold_only_complete_mask].copy()
    gold_status_not_completed = gold_only_complete[~gold_only_complete_mask].copy()
    return gold_status_completed, gold_status_not_completed

# ===========================================================================
# Task G4: Build city/date sales report, agg., by total_revenue, sort by total_revenue and count orders.
# ===========================================================================

def city_report(gold_status_completed):

    gold_city_report = gold_status_completed.copy()

    city_report = gold_city_report.groupby(["run_date", "city"], as_index = False).agg({"line_revenue_gbp": "sum", "order_id": "count"}).sort_values("line_revenue_gbp", ascending = False)
    # The above is clean but it is worth renaming the columns in this report and due to 'adding decimals together can bring back tiny errors because computers store decimals in binary', so i need to do the round again after the agg. 
    # An alternative way of doing the above that created columns as it aggregates, so G1 and G4 is in 26.11.05 Q10.
    city_report = city_report.rename(columns={"line_revenue_gbp": "completed_revenue_gbp", "order_id": "completed_orders"})
    city_report["completed_revenue_gbp"] = city_report["completed_revenue_gbp"].round(2)
    return city_report

# ===========================================================================
# Task G5: Build daily summary report, one row of headline numbers for the day.
# ===========================================================================

def daily_summary(gold_status_completed, run_date, orders_rejected_count):

    gold_daily_summary = gold_status_completed.copy()

    gold_top_order = gold_daily_summary.loc[gold_daily_summary["line_revenue_gbp"].idxmax()]
    daily_summary_report = pd.DataFrame([{
        "run_date": run_date,
        "completed_orders": len(gold_daily_summary),
        "completed_revenue_gbp": round(gold_daily_summary["line_revenue_gbp"].sum(), 2),
        "top_order_id": gold_top_order["order_id"],
        "top_order_revenue_gbp": gold_top_order["line_revenue_gbp"],
        "rejected_orders": orders_rejected_count,
    }])
    # '.idxmax()' finds the row number of the biggest line_revenue_gbp, and '.loc[...]' pulls out that whole row. Like ORDER BY ... DESC LIMIT 1.
    # 'pd.DataFrame([{...}])' builds a one row table, each key is a column. Round again after the sum, same reason as G4.
    return daily_summary_report

# ===========================================================================
# Task G6: Gold checks. Silver valid orders = completed + not completed.
# ===========================================================================

def gold_checks(run_date, silver_valid, gold_status_completed, gold_status_not_completed):

    if len(silver_valid) != len(gold_status_completed) + len(gold_status_not_completed):
        raise ValueError(f"[{run_date}] Gold does not reconcile: silver valid {len(silver_valid)}, "
                         f"completed {len(gold_status_completed)}, not completed {len(gold_status_not_completed)}")
    print(f"[{run_date}] Gold: silver valid {len(silver_valid)} = completed {len(gold_status_completed)} + not completed {len(gold_status_not_completed)}")

# ===========================================================================
# Task G7: Save the city report and daily summary report for the day.
# ===========================================================================
 
def write_gold_files(run_date, city_report, daily_summary_report):
 
    paths = get_paths(run_date)
    paths["gold"].mkdir(parents=True, exist_ok=True)
 
    city_report.to_csv(paths["gold"] / "city_report.csv", index=False)
    daily_summary_report.to_csv(paths["gold"] / "daily_summary_report.csv", index=False)
    # Saves to 04_gold/<run_date>/. mkdir makes the day folder, exist_ok=True carries on if it is already there.
    print(f"[{run_date}] Wrote gold reports to 04_gold/{run_date}/")
 
# ===========================================================================
# Task G8: Build the audit record - counts, reconciliation, revenue, time, status.
# ===========================================================================
 
def build_audit(run_date, bronze_customers, silver_customers_valid, silver_customers_rejected,
                bronze_orders, silver_valid, silver_orders_rejected,
                gold_status_completed, gold_status_not_completed, city_report, daily_summary_report):
 
    customers_rejected_by_reason = {reason: int(count) for reason, count in silver_customers_rejected["rejection_reason"].value_counts().items()}
    orders_rejected_by_reason = {reason: int(count) for reason, count in silver_orders_rejected["rejection_reason"].value_counts().items()}
    # value_counts() counts how many rows have each reason. Like GROUP BY rejection_reason with COUNT(*).
    # int(...) turns pandas' number type into a normal Python number, because json can't save pandas' numbers.
 
    audit = {
        "run_date": run_date,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "customers": {
            "source": len(bronze_customers),
            "valid": len(silver_customers_valid),
            "rejected": len(silver_customers_rejected),
            "rejected_by_reason": customers_rejected_by_reason,
            "reconciles": len(bronze_customers) == len(silver_customers_valid) + len(silver_customers_rejected),
        },
        "orders": {
            "source": len(bronze_orders),
            "valid": len(silver_valid),
            "rejected": len(silver_orders_rejected),
            "rejected_by_reason": orders_rejected_by_reason,
            "reconciles": len(bronze_orders) == len(silver_valid) + len(silver_orders_rejected),
        },
        "output": {
            "completed_orders": len(gold_status_completed),
            "not_completed_orders": len(gold_status_not_completed),
            "completed_revenue_gbp": float(daily_summary_report["completed_revenue_gbp"].iloc[0]),
            "cities_in_report": len(city_report),
        },
        "status": "pending_quality_check",
    }
    # A dictionary inside a dictionary, so the json file is grouped into customers, orders and output.
    # Status stays "pending_quality_check" until the quality gate runs its checks and changes it to success or failed.
    return audit
 
# ===========================================================================
# Task G9: Save the audit record as a json file.
# ===========================================================================
 
def write_audit(run_date, audit):
 
    paths = get_paths(run_date)
    paths["audit"].parent.mkdir(parents=True, exist_ok=True)
 
    paths["audit"].write_text(json.dumps(audit, indent=2))
    # paths["audit"] is a file (06_audit/<run_date>.json), not a folder, so .parent makes the 06_audit folder.
    # json.dumps turns the dictionary into text, indent=2 makes it readable when opened.
    print(f"[{run_date}] Wrote audit to 06_audit/{run_date}.json")
 
# ===========================================================================
# Task G10: The gold task: read bronze and silver, run G1 to G9 in order. This is what Airflow calls.
# ===========================================================================
 
def run_gold(run_date):
 
    paths = get_paths(run_date)
    bronze_customers = pd.read_csv(paths["bronze"] / "customers.csv", dtype=str)
    bronze_orders = pd.read_csv(paths["bronze"] / "orders.csv", dtype=str)
    silver_customers_valid = pd.read_csv(paths["silver_valid"] / "customers.csv")
    silver_customers_rejected = pd.read_csv(paths["silver_rejected"] / "customers.csv")
    silver_valid = pd.read_csv(paths["silver_valid"] / "orders.csv")
    silver_orders_rejected = pd.read_csv(paths["silver_rejected"] / "orders.csv")
    # Bronze is only read here for its row counts (the audit's source numbers), so dtype=str is fine.
    # Silver is read without dtype=str so quantity and price come in as numbers for G1.
 
    gold_add_revenue = add_revenue(silver_valid)
    gold_add_run_date = add_run_date(gold_add_revenue, run_date)
    gold_status_completed, gold_status_not_completed = only_complete(gold_add_run_date)
    gold_city_report = city_report(gold_status_completed)
    daily_summary_report = daily_summary(gold_status_completed, run_date, len(silver_orders_rejected))
 
    gold_checks(run_date, silver_valid, gold_status_completed, gold_status_not_completed)
 
    write_gold_files(run_date, gold_city_report, daily_summary_report)
 
    audit = build_audit(run_date, bronze_customers, silver_customers_valid, silver_customers_rejected,
                        bronze_orders, silver_valid, silver_orders_rejected,
                        gold_status_completed, gold_status_not_completed, gold_city_report, daily_summary_report)
    write_audit(run_date, audit)

# ===========================================================================
# QUALITY GATE: runs as its own Airflow task after run_gold.
# QC1 Quality checks, QC2 Freshness check, QC3 Update the audit, QC4 run_quality_check.
# ===========================================================================

# ===========================================================================
# Task QC1: Quality checks - do the numbers agree across silver, gold and the audit?
# ===========================================================================

def quality_checks(silver_valid, gold_city_report, gold_daily_summary_report, audit):

    qc_silver_valid = silver_valid.copy()

    qc_silver_valid["line_revenue_gbp"] = (qc_silver_valid["quantity"] * qc_silver_valid["unit_price_gbp"]).round(2)
    silver_revenue = round(float(qc_silver_valid.loc[qc_silver_valid["status"] == "COMPLETED", "line_revenue_gbp"].sum()), 2)
    city_report_revenue = round(float(gold_city_report["completed_revenue_gbp"].sum()), 2)
    daily_summary_revenue = round(float(gold_daily_summary_report["completed_revenue_gbp"].iloc[0]), 2)
    audit_revenue = round(float(audit["output"]["completed_revenue_gbp"]), 2)
    # ---------- CODE EXPLANATION ----------
    # QC1 is handed four things that QC4 has read back from disk: the silver valid orders, the two gold reports and
    # the audit. It works on a copy so the original isn't changed. Silver has no revenue column, so line_revenue_gbp
    # is worked out again (quantity x unit price, rounded to pence), the same calculation as G1 but done independently
    # so gold can be checked against the source. silver_revenue reads from the inside out: the mask picks only
    # COMPLETED orders, .loc[mask, "line_revenue_gbp"] takes the revenue for just those rows, .sum() adds it up,
    # float(...) makes it a normal Python number and round(..., 2) rounds it to pence. city_report_revenue adds up the
    # revenue for every city in the city report. The daily summary only has one row, so .iloc[0] takes the value from
    # that first row. The audit is a dictionary inside a dictionary, so audit["output"] opens the output section and
    # ["completed_revenue_gbp"] takes the figure from it. That gives four revenue figures from four different places.

    quality_check_results = {
        "customer counts reconcile": bool(audit["customers"]["reconciles"]),
        "order counts reconcile": bool(audit["orders"]["reconciles"]),
        "no duplicate order_id in silver valid orders": bool(qc_silver_valid["order_id"].is_unique),
        "no missing quantity or price in silver valid orders": bool(qc_silver_valid[["quantity", "unit_price_gbp"]].notna().all().all()),
        "revenue: silver = city report": silver_revenue == city_report_revenue,
        "revenue: city report = daily summary": city_report_revenue == daily_summary_revenue,
        "revenue: daily summary = audit": daily_summary_revenue == audit_revenue,
    }
    return quality_check_results
    # ---------- CODE EXPLANATION ----------
    # A dictionary where each check's name is the key and its result is the value: True means passed, False means
    # failed. The two reconcile checks were already worked out by gold (bronze = valid + rejected) and saved in the
    # audit, so they are just read back. .is_unique is True if every order_id appears only once, which proves silver's
    # duplicate rejection worked. For the missing check, [["quantity", "unit_price_gbp"]] takes just those two columns,
    # .notna() gives True for every cell that isn't missing, the first .all() asks "is every row True?" for each
    # column and the second .all() asks "are both columns True?". The last three compare the revenue figures in a
    # chain, silver -> city report -> daily summary -> audit, so if every link matches, the revenue is the same
    # everywhere. bool(...) turns pandas' True/False into Python's normal True/False, because json can't save pandas'.

# ===========================================================================
# Task QC2: Freshness check - were the gold reports written by this run?
# ===========================================================================

def freshness_check(run_date):

    paths = get_paths(run_date)
    FRESHNESS_MAX_AGE_MINUTES = 60

    now = datetime.now()
    output_age_minutes = {}
    for file_name in ["city_report.csv", "daily_summary_report.csv"]:
        modified = datetime.fromtimestamp((paths["gold"] / file_name).stat().st_mtime)
        output_age_minutes[file_name] = round((now - modified).total_seconds() / 60, 1)
    outputs_are_fresh = all(age <= FRESHNESS_MAX_AGE_MINUTES for age in output_age_minutes.values())
    print(f"[{run_date}] Gold report age in minutes: {output_age_minutes} (limit {FRESHNESS_MAX_AGE_MINUTES})")
    return output_age_minutes, outputs_are_fresh
    # ---------- CODE EXPLANATION ----------
    # QC2 only needs the date, to find that day's gold folder with get_paths. FRESHNESS_MAX_AGE_MINUTES = 60 is the
    # limit: a gold report older than 60 minutes counts as a leftover from an earlier run, not this one (lab 2 Q14,
    # Q15). now is the date and time at the moment the check runs. The loop goes through the two gold reports one at a
    # time. .stat() gets information about the file from the computer, .st_mtime is when it was last saved (stored as
    # a long number of seconds) and datetime.fromtimestamp(...) turns that number into a normal date and time.
    # now - modified gives how long ago it was saved, .total_seconds() turns that into seconds, / 60 into minutes and
    # round(..., 1) rounds to one decimal place, stored under the file's name, e.g. {"city_report.csv": 0.5}.
    # all(...) is True only if every file is within the limit. It hands back two things: the ages (for the audit)
    # and the overall fresh or not result (for the checks).

# ===========================================================================
# Task QC3: Update the audit - add the check results and set status to success or failed.
# ===========================================================================

def update_audit(run_date, audit, quality_check_results, output_age_minutes):

    paths = get_paths(run_date)

    audit["output_age_minutes"] = output_age_minutes
    audit["quality_checks"] = quality_check_results
    audit["status"] = "success" if all(quality_check_results.values()) else "failed"
    paths["audit"].write_text(json.dumps(audit, indent=2))
    for name, passed in quality_check_results.items():
        print(f"[{run_date}] {'PASS' if passed else 'FAIL'}  {name}")
    return audit
    # ---------- CODE EXPLANATION ----------
    # QC3 is handed the audit, the check results (QC1 plus the freshness result) and the file ages from QC2. The first
    # two lines add two new sections to the audit dictionary: how old the reports were, and the result of every check.
    # all(...) is True only if every check passed, so the status becomes "success", and if even one failed it becomes
    # "failed". This replaces gold's "pending_quality_check". write_text(json.dumps(...)) saves the updated audit back
    # over the old one, the same as G9. The audit is saved even when a check fails, so the evidence is kept. The loop
    # prints PASS or FAIL next to every check's name, so the results show in the terminal or the Airflow log. It hands
    # back the updated audit so QC4 can check its status.

# ===========================================================================
# Task QC4: The quality gate task: read the files back, run QC1 to QC3, stop if anything failed. This is what Airflow calls.
# ===========================================================================

def run_quality_check(run_date):

    paths = get_paths(run_date)

    required_files = [
        paths["silver_valid"] / "orders.csv",
        paths["gold"] / "city_report.csv",
        paths["gold"] / "daily_summary_report.csv",
        paths["audit"],
    ]
    missing = [str(file_path) for file_path in required_files if not file_path.exists()]
    if missing:
        raise FileNotFoundError(f"[{run_date}] Quality gate can't run, missing file(s): {missing}")
    # ---------- CODE EXPLANATION ----------
    # Before reading anything, checks that every file the quality gate needs is actually there, like Anyelle's
    # required_files check. The one line loop builds a list of every file that does NOT exist (.exists() is False),
    # the same pattern as the missing list in bronze B1. If the list isn't empty, raise stops the run with a clear
    # message naming the missing files, instead of a less clear pandas error from read_csv further down.

    silver_valid = pd.read_csv(paths["silver_valid"] / "orders.csv")
    gold_city_report = pd.read_csv(paths["gold"] / "city_report.csv")
    gold_daily_summary_report = pd.read_csv(paths["gold"] / "daily_summary_report.csv")
    audit = json.loads(paths["audit"].read_text())
    # ---------- CODE EXPLANATION ----------
    # Reads everything back from disk, so it checks what was actually saved, not what was held in memory. The silver
    # valid orders and the two gold reports are read with pd.read_csv. For the audit, .read_text() reads the file as
    # text and json.loads(...) turns that text back into a Python dictionary, the reverse of json.dumps in G9.

    quality_check_results = quality_checks(silver_valid, gold_city_report, gold_daily_summary_report, audit)
    output_age_minutes, outputs_are_fresh = freshness_check(run_date)
    quality_check_results["gold reports are fresh (written in the last 60 min)"] = outputs_are_fresh

    audit = update_audit(run_date, audit, quality_check_results, output_age_minutes)

    if audit["status"] != "success":
        failed = [name for name, passed in quality_check_results.items() if not passed]
        raise ValueError(f"[{run_date}] Quality checks failed: {failed}")
    print(f"[{run_date}] All quality checks passed. Audit status: success")
    # ---------- CODE EXPLANATION ----------
    # Runs QC1 to get the 7 check results, then QC2 to get the file ages and the fresh or not result. The freshness
    # result is added to the dictionary as an 8th check, so it is treated exactly like the others. QC3 then updates
    # and saves the audit and prints PASS or FAIL for each check. If the status isn't "success", a one line loop
    # (like Anyelle's missing list in bronze) builds a list of the names of the checks that failed, and raise stops
    # the run with an error naming them. Airflow marks the task red and platinum never runs, so nothing is published.
    # This happens after the audit is saved, so the failure is recorded first. The last print is only reached if
    # every check passed.


# ===========================================================================
# Task G11: Final run, only when I start this file myself in the terminal.
# ===========================================================================
 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please give a date, e.g. python 02_scripts/gold.py 20261105")
        sys.exit(1)
    try:
        run_gold(sys.argv[1])
        run_quality_check(sys.argv[1])
    except Exception as Error:
        print(f"Error running run_gold: {Error}")
        raise
    print("(DB) Finished running.")