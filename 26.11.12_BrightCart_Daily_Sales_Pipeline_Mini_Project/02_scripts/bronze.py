"""
BrightCart Daily Sales Pipeline: bronze layer.
 
Lands the raw files in the pipeline, unchanged.
    Reads:   01_data/01_raw/<run_date>/customers.csv, orders.csv
    Writes:  01_data/02_bronze/<run_date>/customers.csv, orders.csv
 
What bronze does:
    B1       Checks both input files exist and are not empty, and counts the source rows.
             If a file is missing or empty, it stops the run with a clear error.
    B2       Reads one raw file into pandas as text, exactly as received.
    B3       Writes one file to bronze unchanged: no columns added, nothing fixed.
    B4       Counts the rows in one bronze file, by reading it back from disk.
    B5       Checks source rows = bronze rows for each file. Stops the run if not.
    B6       run_bronze: runs B1 to B5 in order. This is what Airflow calls.
 
Settings used only by bronze:
    INPUT_FILES (the two files the shop system sends every day)
 
Run it on its own from the data_course folder:
    python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/bronze.py 20261105
"""
 
# ===========================================================================
# Imports and Settings
# ===========================================================================
 
import sys
 
import pandas as pd
 
from config import get_paths
 
INPUT_FILES = ["customers.csv", "orders.csv"]
# CAPITALS is a Python naming habit that signals "this is a setting that shouldn't change while the program runs". These are called constants.
 
# ===========================================================================
# Configuration
# ===========================================================================
 
# N/A
 
# ===========================================================================
# Task B1: Check both input files exist and are not empty, and count the source rows.
# ===========================================================================
 
def check_input_files(run_date):
 
    paths = get_paths(run_date)
 
    missing = [name for name in INPUT_FILES if not (paths["raw"] / name).exists()]
    if missing:
        raise FileNotFoundError(f"[{run_date}] Missing input file(s) in {paths['raw']}: {missing}")
    # Anyelle's one line loop: builds a list of every file that is NOT there, so the error names all missing files at once.
 
    bronze_source_counts = {}
    for name in INPUT_FILES:
        try:
            rows = len(pd.read_csv(paths["raw"] / name, dtype=str))
        except pd.errors.EmptyDataError:
            raise ValueError(f"[{run_date}] Input file is empty: {name}")
        if rows == 0:
            raise ValueError(f"[{run_date}] Input file is empty: {name}")
        bronze_source_counts[name] = rows
    # EmptyDataError catches a completely blank file (0 bytes), which pandas can't read at all.
    # rows == 0 catches a file with a header line but no data rows.
 
    print(f"[{run_date}] Input files found: {bronze_source_counts}")
    return bronze_source_counts
 
# ===========================================================================
# Task B2: Read one raw file into pandas as text, exactly as received.
# ===========================================================================
 
def read_raw_file(run_date, file_name):
 
    paths = get_paths(run_date)
 
    bronze_read_raw = pd.read_csv(paths["raw"] / file_name, dtype=str, keep_default_na=False)
    # dtype=str keeps every value as text, so "25.00" is not turned into a number.
    # keep_default_na=False stops pandas turning text like "NA" or "None" into empty values.
    return bronze_read_raw
 
# ===========================================================================
# Task B3: Write one file to bronze unchanged.
# ===========================================================================
 
def write_bronze_file(bronze_read_raw, run_date, file_name):
 
    paths = get_paths(run_date)
    paths["bronze"].mkdir(parents=True, exist_ok=True)
 
    bronze_read_raw.to_csv(paths["bronze"] / file_name, index=False)
    # index=False stops pandas adding its own row number column.
    print(f"[{run_date}] Wrote bronze file: {file_name}")
 
# ===========================================================================
# Task B4: Count the rows in one bronze file, by reading it back from disk.
# ===========================================================================
 
def count_bronze_file(run_date, file_name):
 
    paths = get_paths(run_date)
 
    bronze_rows = len(pd.read_csv(paths["bronze"] / file_name, dtype=str, keep_default_na=False))
    # Reads the saved file back, so it proves what was actually written, not just what pandas was holding.
    return bronze_rows
 
# ===========================================================================
# Task B5: Bronze checks. Source rows = bronze rows for each file.
# ===========================================================================
 
def bronze_count_checks(run_date, bronze_source_counts, bronze_counts):
 
    for file_name in INPUT_FILES:
        if bronze_source_counts[file_name] != bronze_counts[file_name]:
            raise ValueError(f"[{run_date}] Row count mismatch for {file_name}: "
                             f"source {bronze_source_counts[file_name]}, bronze {bronze_counts[file_name]}")
    print(f"[{run_date}] Bronze: source rows {bronze_source_counts}, bronze rows {bronze_counts} (match)")
 
# ===========================================================================
# Task B6: The bronze task: check the files, land each one, count and compare. This is what Airflow calls.
# ===========================================================================
 
def run_bronze(run_date):
 
    bronze_source_counts = check_input_files(run_date)
 
    bronze_counts = {}
    for file_name in INPUT_FILES:
        bronze_read_raw = read_raw_file(run_date, file_name)
        write_bronze_file(bronze_read_raw, run_date, file_name)
        bronze_counts[file_name] = count_bronze_file(run_date, file_name)
    # Runs B2 to B4 once for customers.csv, then once for orders.csv.
 
    bronze_count_checks(run_date, bronze_source_counts, bronze_counts)
    return {"source": bronze_source_counts, "bronze": bronze_counts}
 
# ===========================================================================
# Task B7: Final run, only when I start this file myself in the terminal.
# ===========================================================================
 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please give a date, e.g. python 02_scripts/bronze.py 20261105")
        sys.exit(1)
    try:
        run_bronze(sys.argv[1])
    except Exception as Error:
        print(f"Error running run_bronze: {Error}")
        raise
    print("(DB) Finished running.")