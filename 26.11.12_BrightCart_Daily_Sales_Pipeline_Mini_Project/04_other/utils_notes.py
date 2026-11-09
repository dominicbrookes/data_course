# ===========================================================================
# utils.py: what it is, and what mine would look like
# ===========================================================================
# Every line in this file is a comment (starts with #), so nothing here runs.
# This is a reference note only. My pipeline does NOT use a utils.py.


# ===========================================================================
# Part 1: What a utils.py file does
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# utils is short for "utilities". A utils.py is an ordinary Python file that holds small helper functions which
# several scripts need, so each helper is written once and imported everywhere, instead of being copied into every
# file. There is nothing special about the name: Python treats it like any other .py file. It's just a convention
# that tells other people "the shared helpers are in here".
#
# It's useful because:
#     1. Less repetition: the same five lines aren't copied into four scripts.
#     2. One place to fix: if a helper has a bug, fixing it once fixes it for every script that uses it.
#     3. Consistency: every layer saves files and prints messages in exactly the same way.
#
# config.py vs utils.py: config holds SETTINGS (paths, limits, lists like VALID_STATUSES), utils holds ACTIONS
# (functions that do a job). My get_paths sits in config because it only works out addresses, it doesn't do anything.
#
# SQL comparison: utils is like a set of stored procedures or user-defined functions that several queries call,
# instead of repeating the same block of SQL in every query.
#
# The catch: utils.py can turn into a "junk drawer" where unrelated bits pile up because there was nowhere else to
# put them. A good rule: only move something into utils when at least two scripts genuinely use it.


# ===========================================================================
# Part 2: What repeats in my project now (no utils.py)
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# Three patterns are written out again in several scripts:
#
#     1. Saving a CSV: make the day folder, save with index=False, print a message.
#        bronze B3, silver S13 (x4), gold G7 (x2)
#
#             paths["bronze"].mkdir(parents=True, exist_ok=True)
#             bronze_read_raw.to_csv(paths["bronze"] / file_name, index=False)
#
#     2. Reading a saved file back to count its rows, as text, exactly as saved.
#        bronze B4, and the same idea in the silver and gold count checks
#
#             len(pd.read_csv(paths["bronze"] / file_name, dtype=str, keep_default_na=False))
#
#     3. The final run block: check a date was given, try, print the error and raise, print "Finished running".
#        bronze B7, silver S16, platinum P5 (gold G11 is the same but runs two functions)


# ===========================================================================
# Part 3: What mine would look like
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# Step 1: create 02_scripts/utils.py, next to config.py, so the scripts can import it the same way they import
# config (from utils import ...). It would look like this:
#
#     """
#     BrightCart Daily Sales Pipeline: shared helper functions.
#
#     Small jobs that more than one layer does, written once:
#         U1   save_csv: make the day folder, save a table as CSV, print a message.
#         U2   count_rows: read a saved CSV back and count its rows.
#         U3   run_from_terminal: the final run block every layer uses.
#     """
#
#     import sys
#
#     import pandas as pd
#
#     # Task U1: Save a table as a CSV file in a day folder.
#     def save_csv(table, folder, file_name, run_date):
#         folder.mkdir(parents=True, exist_ok=True)
#         table.to_csv(folder / file_name, index=False)
#         print(f"[{run_date}] Wrote {file_name} to {folder.name}")
#
#     # Task U2: Read a saved CSV back and count its rows.
#     def count_rows(file_path):
#         return len(pd.read_csv(file_path, dtype=str, keep_default_na=False))
#
#     # Task U3: The final run block, when a layer is started from the terminal.
#     def run_from_terminal(run_function, script_name):
#         if len(sys.argv) < 2:
#             print(f"Please give a date, e.g. python 02_scripts/{script_name} 20261105")
#             sys.exit(1)
#         try:
#             run_function(sys.argv[1])
#         except Exception as Error:
#             print(f"Error running {run_function.__name__}: {Error}")
#             raise
#         print("(DB) Finished running.")
#
# U1 takes the table, the folder (e.g. paths["bronze"]), the file name and the date, and does the three lines every
# layer repeats. U2 is bronze B4 with the path passed in, so any layer can use it. U3 takes a FUNCTION as its input
# (run_function, with no brackets, so it isn't run yet), the same idea as the list of run functions in
# 01_optional_manual_run.py. run_function.__name__ is the function's own name, e.g. "run_bronze", for the error.
#
# Step 2: the scripts would use it. For example, bronze would change like this:
#
#     from config import get_paths
#     from utils import save_csv, count_rows, run_from_terminal
#
#     # B3 shrinks to one line inside write_bronze_file:
#     save_csv(bronze_read_raw, paths["bronze"], file_name, run_date)
#
#     # B4 shrinks to one line inside count_bronze_file:
#     bronze_rows = count_rows(paths["bronze"] / file_name)
#
#     # B7, the final run block, shrinks to:
#     if __name__ == "__main__":
#         run_from_terminal(run_bronze, "bronze.py")
#
# Gold is the exception for U3, because its final run block runs run_gold AND run_quality_check. It would either
# keep its own final run block, or use a small function that runs both, e.g.
#     def run_gold_and_quality_check(run_date):
#         run_gold(run_date)
#         run_quality_check(run_date)
# and pass that into run_from_terminal instead.


# ===========================================================================
# Part 4: Why I didn't do this
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# Each layer is self-contained: I can open bronze.py and see every step it does, top to bottom, without jumping
# to another file. While I'm learning, that is easier to read and explain than helpers hidden in utils.py. The
# pipeline already works and is tested end to end, so I kept the repetition and noted it as a possible improvement
# in the README instead.