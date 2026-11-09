# ===========================================================================
# __init__.py: what it is, and what mine would look like
# ===========================================================================
# Every line in this file is a comment (starts with #), so nothing here runs.
# This is a reference note only. My pipeline does NOT use an __init__.py.


# ===========================================================================
# Part 1: What an __init__.py file does
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# An __init__.py is a file you put inside a folder to tell Python "treat this folder as a package". A package is
# just a folder of .py files that other code can import from by the folder's name, e.g.
# from brightcart_pipeline.bronze import run_bronze reads as "go into the brightcart_pipeline folder, open bronze.py,
# and take run_bronze". Without it (or a sys.path trick), Python only looks for imports in the folder the running
# file is in and in the installed libraries, so it can't see a folder of my own scripts somewhere else.
#
# The file can be completely empty. Its existence is enough. Anything written inside it runs once, the first time
# the package is imported, so it is often used to:
#     1. Do nothing (empty file): the most common choice.
#     2. Shortcut imports: bring the most used functions up to the package level, so other code can write
#        from brightcart_pipeline import run_bronze instead of from brightcart_pipeline.bronze import run_bronze.
#     3. List what the package offers with __all__ (a list of names), like a table of contents.
#
# SQL comparison: a package is like a schema. bronze.py, silver.py and gold.py are like tables inside it, and the
# import is like writing schema.table. The __init__.py is what makes the schema exist, so the names can be found.
#
# Since Python 3.3 a folder can sometimes be imported without an __init__.py ("namespace packages"), but adding one
# is still the normal, clear way to do it, and tools like VS Code's checker understand it better.


# ===========================================================================
# Part 2: How my project does it now (no __init__.py)
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# My DAG adds the scripts folder to Python's search list, then imports the run functions by file name:
#
#     sys.path.insert(0, str(PROJECT_ROOT / "02_scripts"))
#     from bronze import run_bronze                      # noqa: E402
#
# sys.path is the list of folders Python searches when it sees an import. insert(0, ...) puts 02_scripts at the
# front of that list while the DAG runs. This works, and Airflow is happy, but VS Code's checker reads the file
# without running it, so it never sees the sys.path change. That is why the DAG file shows 4 warnings (yellow).


# ===========================================================================
# Part 3: What mine would look like if 02_scripts became a package
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# Step 1: rename the folder. A package name has to be a valid Python name, and Python names can't start with a
# digit, so 02_scripts can't be imported as a package. It would need a name like brightcart_pipeline.
# (01_optional_manual_run.py is fine as it is, because nothing imports it: it is only ever run directly.)
#
# Step 2: add brightcart_pipeline/__init__.py. Mine would look like this:
#
#     """
#     BrightCart Daily Sales Pipeline: the pipeline package.
#
#     Brings the run function from each layer up to the package level, so the DAG and the manual run script
#     can import them all from one place.
#     """
#
#     from .bronze import run_bronze
#     from .silver import run_silver
#     from .gold import run_gold, run_quality_check
#     from .platinum import run_platinum
#
#     __all__ = ["run_bronze", "run_silver", "run_gold", "run_quality_check", "run_platinum"]
#
# The dot in from .bronze means "bronze.py in THIS same folder" (a relative import). __all__ lists the names the
# package offers. The same five functions the DAG calls today, just collected in one place.
#
# Step 3: change the imports inside the scripts. Every script that does from config import get_paths would become
# from .config import get_paths, with a dot, for the same reason as above. One knock-on effect: once a script uses
# a dot import, it can't be started with python bronze.py any more (Python complains there's no parent package).
# It would be run as a module from the project folder instead: python -m brightcart_pipeline.bronze 20261105
#
# Step 4: change the DAG imports. sys.path would point at the project folder (the folder that CONTAINS the package)
# instead of at the scripts folder, and the five imports become one line:
#
#     sys.path.insert(0, str(PROJECT_ROOT))
#     from brightcart_pipeline import run_bronze, run_silver, run_gold, run_quality_check, run_platinum
#
# In industry the sys.path line usually disappears too: the package is installed into the environment once
# (pip install -e . with a small pyproject.toml file), and then any file, including the DAG, can import it
# anywhere, the same way it imports pandas.


# ===========================================================================
# Part 4: Why I didn't do this
# ===========================================================================

# ---------- CODE EXPLANATION ----------
# Changing to a package means renaming 02_scripts and changing the imports in every script, the DAG and the manual
# run file, then re-testing everything. My pipeline already works and is tested end to end, so I kept sys.path and
# noted it as a known limitation in the README instead.