"""
BrightCart Daily Sales Pipeline: shared configuration.

Holds only the settings that more than one script needs.
Settings used by just one script live at the top of that script:
    INPUT_FILES               -> bronze.py
    VALID_STATUSES            -> silver.py
    REVENUE_STATUS            -> gold.py (platinum.py imports it from gold)
    FRESHNESS_MAX_AGE_MINUTES -> gold.py

Each script imports what it needs, for example:
    from config import get_paths
"""

from pathlib import Path
# ---------- CODE EXPLANATION ----------
# # What Path does for you:
# Without it, a file path is just a piece of text, like "01_data/01_raw/20261105/orders.csv".
# Python doesn't know it's a path, so you'd have to glue folder names together
# yourself and remember whether to use a forward slash or a backslash.


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "01_data"
# ---------- CODE EXPLANATION ----------
# These two lines work out where the project's data folder is, without typing the full address by hand.
# Path(__file__) gives the full location of config.py itself, and .resolve() makes sure it's the complete
# address from the top of the computer:
# /home/dominicbrookes/data_course/26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/config.py
# .parents[1] then goes up two levels: .parents[0] is the 02_scripts folder, and .parents[1] is the BrightCart
# project folder, which is saved as PROJECT_ROOT. The second line adds 01_data onto the end to make DATA_DIR,
# which every other path in the pipeline is built from. Path(__file__) doesn't lock in the folder the terminal
# is currently in; it locks in where config.py lives. Because config.py is always in 02_scripts, PROJECT_ROOT
# always comes out as the BrightCart folder, whether the script is run from data_course, from the project
# folder, on another computer, or by Airflow from its own folder. A plain path like Path("01_data") would
# depend on where the terminal is, and would break if run from the wrong folder.


def get_paths(run_date):
    """Return every folder and file used for one day of data (run_date = "YYYYMMDD")."""
    return {
        "raw": DATA_DIR / "01_raw" / run_date,
        "bronze": DATA_DIR / "02_bronze" / run_date,
        "silver_valid": DATA_DIR / "03_silver" / "01_valid" / run_date,
        "silver_rejected": DATA_DIR / "03_silver" / "02_rejected" / run_date,
        "gold": DATA_DIR / "04_gold" / run_date,
        "platinum": DATA_DIR / "05_platinum" / run_date,
        "audit": DATA_DIR / "06_audit" / f"{run_date}.json",
    }
# ---------- CODE EXPLANATION ----------
# get_paths is the address book for the pipeline: it takes a date as text, like "20261105", and returns a
# dictionary with the folder for every layer on that day. Each entry has a label on the left ("raw", "bronze",
# "silver_valid" and so on) and a path on the right, built by starting at DATA_DIR (the 01_data folder), adding
# the layer folder, then adding the date, so "bronze" becomes .../01_data/02_bronze/20261105. The silver entries
# have one extra folder in the middle (01_valid or 02_rejected). The audit entry is a file rather than a folder:
# the f before the quotes makes it an f-string, which drops the date into the curly brackets to make
# 20261105.json. Each script calls get_paths once at the start, e.g. paths = get_paths("20261105"), then looks up
# the folder it needs by its label, e.g. paths["bronze"]. Because every script uses the same address book, bronze
# writes to exactly the folder that silver reads from. Calling get_paths doesn't create any folders; it only works
# out the addresses. The folders are made later, when a script calls .mkdir() just before saving a file there.