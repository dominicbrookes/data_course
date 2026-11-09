"""
BrightCart Daily Sales Pipeline: platinum layer.
 
Automatic data quality report: compares the RAW orders with the CLEANED (silver valid) orders for one day,
side by side, using Sweetviz. One line of code builds the whole report: statistics, charts and missing values
per column. This mirrors Anyelle's optional_data_report.py, applied to my folders.
    Reads:   01_data/01_raw/<run_date>/orders.csv
             01_data/03_silver/01_valid/<run_date>/orders.csv
    Writes:  01_data/05_platinum/<run_date>/orders_raw_vs_cleaned.html
 
What platinum does:
    P1       Checks the cleaned orders file exists. If silver hasn't run for the day, it stops with a clear message.
    P2       Reads the raw orders (default types, on purpose) and the cleaned orders.
    P3       Builds the Sweetviz comparison report and saves it.
    P4       run_platinum: runs P1 to P3 in order.
 
Final run:
    P5       Runs run_platinum when the file is started from the terminal (skipped when Airflow imports it).
 
Install once (in the venv):
    pip install sweetviz
 
Run it on its own from the data_course folder, after the pipeline has processed the day:
    python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/platinum.py 20261105
Then open 01_data/05_platinum/20261105/orders_raw_vs_cleaned.html in the browser.
"""
 
# ===========================================================================
# Imports and Settings
# ===========================================================================
 
import sys
 
import pandas as pd
import sweetviz as sv
 
from config import get_paths
 
# ===========================================================================
# Configuration
# ===========================================================================
 
# N/A
 
# ===========================================================================
# Task P1: Check the cleaned orders file exists.
# ===========================================================================
 
def check_cleaned_file(run_date):
 
    paths = get_paths(run_date)
 
    if not (paths["silver_valid"] / "orders.csv").exists():
        raise FileNotFoundError(f"[{run_date}] Run the pipeline for {run_date} first: no cleaned orders in 03_silver/01_valid/{run_date}/")
    # ---------- CODE EXPLANATION ----------
    # The report compares raw with cleaned, so it can't run until silver has produced the cleaned orders for the day.
    # .exists() is False if the file isn't there, and raise stops with a message saying what to do. Anyelle uses
    # sys.exit for this; raise is used here so it behaves like the rest of my scripts (and Airflow marks it failed).
 
# ===========================================================================
# Task P2: Read the raw orders and the cleaned orders.
# ===========================================================================
 
def read_orders(run_date):
 
    paths = get_paths(run_date)
 
    platinum_raw_orders = pd.read_csv(paths["raw"] / "orders.csv")
    platinum_clean_orders = pd.read_csv(paths["silver_valid"] / "orders.csv")
    return platinum_raw_orders, platinum_clean_orders
    # ---------- CODE EXPLANATION ----------
    # Raw is read with pandas' default types on purpose (no dtype=str), so the report shows how messy it is, e.g.
    # quantity comes in as text because of values like "two", and prices as text because of the £. The cleaned orders
    # are the silver valid file, the same file Anyelle uses (her orders_valid.csv). It hands back both tables.
 
# ===========================================================================
# Task P3: Build the Sweetviz comparison report and save it.
# ===========================================================================
 
def build_report(run_date, platinum_raw_orders, platinum_clean_orders):
 
    paths = get_paths(run_date)
 
    report = sv.compare([platinum_raw_orders, "Raw orders"], [platinum_clean_orders, "Cleaned orders"])
 
    paths["platinum"].mkdir(parents=True, exist_ok=True)
    report_file = paths["platinum"] / "orders_raw_vs_cleaned.html"
    report.show_html(str(report_file), open_browser=False, layout="vertical")
    print(f"[{run_date}] Report saved to 05_platinum/{run_date}/orders_raw_vs_cleaned.html")
    # ---------- CODE EXPLANATION ----------
    # sv.compare(...) is the automatic part: one call compares both tables column by column, with statistics, charts,
    # missing values and the most common values side by side. Each table is given as [table, "label"], and the label
    # is what the report calls it. mkdir makes the 05_platinum day folder. show_html saves the report as an
    # interactive web page: str(...) turns the Path into text because Sweetviz wants text, open_browser=False stops it
    # opening a browser by itself, and layout="vertical" stacks the columns down the page.
 
# ===========================================================================
# Task P4: The platinum task: run P1 to P3 in order.
# ===========================================================================
 
def run_platinum(run_date):
 
    check_cleaned_file(run_date)
    platinum_raw_orders, platinum_clean_orders = read_orders(run_date)
    build_report(run_date, platinum_raw_orders, platinum_clean_orders)
 
# ===========================================================================
# Task P5: Final run, only when I start this file myself in the terminal.
# ===========================================================================
 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please give a date, e.g. python 02_scripts/platinum.py 20261105")
        sys.exit(1)
    try:
        run_platinum(sys.argv[1])
    except Exception as Error:
        print(f"Error running run_platinum: {Error}")
        raise
    print("(DB) Finished running.")
 