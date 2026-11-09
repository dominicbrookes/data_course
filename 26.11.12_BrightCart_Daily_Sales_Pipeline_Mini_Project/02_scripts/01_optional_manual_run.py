"""
BrightCart Daily Sales Pipeline: run the whole pipeline by hand.

---------------------------------------------------------------------------
WHAT THIS FILE IS
---------------------------------------------------------------------------
A manual version of the scheduler. It runs every layer of the pipeline for one or more days, in the same order
the scheduled workflow uses:

    bronze  >>  silver  >>  gold  >>  quality gate  >>  platinum

It is a "nice to have", not part of the project's scheduled run. It is useful for:
    * testing the whole pipeline quickly, without starting the scheduler
    * re-running a day by hand after fixing a problem (see RECOVERY.md)
    * showing the pipeline works on its own, separate from the scheduler

---------------------------------------------------------------------------
HOW TO RUN IT (from the data_course folder, with the venv active)
---------------------------------------------------------------------------
    Every day found in 01_raw, oldest first:
        python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py

    One day:
        python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py 20261105

    Several days:
        python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/01_optional_manual_run.py 20261104 20261105

---------------------------------------------------------------------------
HOW IT WORKS
---------------------------------------------------------------------------
It doesn't contain any pipeline logic itself. It imports the one "run" function from each layer, the same
functions the scheduler calls, and calls them in order:

    run_bronze(run_date)          from bronze.py    (B6)
    run_silver(run_date)          from silver.py    (S15)
    run_gold(run_date)            from gold.py      (G10)
    run_quality_check(run_date)   from gold.py      (QC4)
    run_platinum(run_date)        from platinum.py  (P4)

Because each layer reads the files the layer before it saved, running them one after another for the same date
is all it takes.

If any step raises an error (e.g. a missing input file in bronze, counts that don't reconcile in silver, or a
failed quality check), the run stops there, so the later steps never run on bad data. This is the same idea as
"upstream_failed" in the scheduler. Because one failing day stops the whole run, if you run several days and one
fails, the days after it are not run either: fix the problem and run again.

Re-running is safe: every layer replaces that day's files rather than adding to them (see the idempotency
section of README.md), so running a day twice gives the same result.

---------------------------------------------------------------------------
HOW IT COMPARES WITH THE SCHEDULED WORKFLOW
---------------------------------------------------------------------------
                                         this file            the scheduler
    runs the layers in order             yes                  yes
    stops later steps if one fails       yes                  yes
    who starts it                        me, in the terminal  the scheduler, on a schedule (daily)
    retries a failed step                no                   yes (retries)
    red / green status per step          no (terminal only)   yes (in the web screen)
    keeps a history and logs of runs     no                   yes
    re-run just one failed step          no (whole day)       yes

---------------------------------------------------------------------------
THINGS TO KNOW
---------------------------------------------------------------------------
    * It imports platinum.py, so Sweetviz must be installed (pip install sweetviz), or none of it will run.
    * Importing the layer files doesn't run them: each one only runs its own "final run" block when it is started
      directly, thanks to if __name__ == "__main__":. So importing them just makes their run functions available.
    * This file deliberately never names the scheduler or its workflow files, so the scheduler's folder scan skips
      it and never imports it. It can't interfere with the scheduled run.

What it does:
    R1       run_day: runs bronze, silver, gold, the quality gate and platinum for one day, in order.
    R2       Final run: works out which days to run, then runs R1 for each one.
"""

# ===========================================================================
# Imports and Settings
# ===========================================================================

import sys

from config import DATA_DIR
from bronze import run_bronze
from silver import run_silver
from gold import run_gold, run_quality_check
from platinum import run_platinum

# ===========================================================================
# Configuration
# ===========================================================================

# N/A

# ===========================================================================
# Task R1: Run every layer for one day, in order.
# ===========================================================================

def run_day(run_date):

    print(f"\n========== BrightCart pipeline: {run_date} ==========")
    for task in [run_bronze, run_silver, run_gold, run_quality_check, run_platinum]:
        print(f"\n> {task.__name__}")
        task(run_date)
    print(f"\nBrightCart pipeline completed successfully for {run_date}")
    # ---------- CODE EXPLANATION ----------
    # The list holds the five run functions themselves, with no brackets, so they aren't run yet: it is just a list
    # of "things to do". The loop takes each one in turn and calls it with task(run_date), so it runs bronze, then
    # silver, then gold, then the quality gate, then platinum. task.__name__ is the function's own name, e.g.
    # "run_bronze", printed so you can see which step is running. If any step raises an error, the loop stops there
    # and the error is passed up, so later steps never run on bad data. This is Anyelle's run_day.

# ===========================================================================
# Task R2: Final run, only when I start this file myself in the terminal.
# ===========================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_dates = sys.argv[1:]
    else:
        run_dates = sorted(folder.name for folder in (DATA_DIR / "01_raw").iterdir() if folder.is_dir())

    for run_date in run_dates:
        run_day(run_date)
    print("(DB) Finished running.")
    # ---------- CODE EXPLANATION ----------
    # sys.argv[1:] is every date typed after the file name, so one or several days can be run. If no date is typed,
    # (DATA_DIR / "01_raw").iterdir() lists everything inside 01_raw, .is_dir() keeps only the folders (the day
    # folders) and sorted(...) puts them in date order, so every day is run oldest first. Then R1 runs each day.