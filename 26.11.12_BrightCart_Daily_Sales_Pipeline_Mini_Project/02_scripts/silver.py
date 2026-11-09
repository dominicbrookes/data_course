"""
BrightCart Daily Sales Pipeline: silver layer.

Fixes safe values, rejects unsafe rows with a reason, and joins valid orders to valid customers.
    Reads:   01_data/02_bronze/<run_date>/customers.csv, orders.csv
    Writes:  01_data/03_silver/01_valid/<run_date>/customers.csv, orders.csv (orders joined to customers)
             01_data/03_silver/02_rejected/<run_date>/customers.csv, orders.csv (with rejection_reason)
             01_data/07_sql_database/brightcart.db (the same four tables, all days, for DBeaver)

What silver does:
    S1-S3    Customers: strip spaces, title-case city, customer_id to a whole number, empty text to missing.
    S4       Customers: reject missing customer_id, missing customer_name, missing city, duplicate customer_id.
    S5-S9    Orders: strip spaces, status to capitals, remove £, convert numbers and IDs,
             empty text to missing, dates from both formats (2026-11-05 and 05/11/2026).
    S10      Orders: reject missing values, duplicate order_id, zero or negative price or quantity,
             invalid status, unknown customer. Every rejected row keeps a rejection_reason.
    S11      Joins valid orders to valid customers (adds customer_name and city).
    S12      Checks the counts: bronze = valid + rejected for customers and orders,
             and the join kept the same number of orders. Stops the run if not.
    S13      Writes the four files (only if S12 passed).
    S14      Loads the four tables into the SQLite database brightcart.db, with a run_date column.
             Re-running a day replaces that day's rows instead of duplicating them.
    S15      run_silver: runs S1 to S14 in order. This is what Airflow calls.

Final run:
    S16      Runs run_silver when the file is started from the terminal (skipped when Airflow imports it).

Settings used only by silver:
    VALID_STATUSES (inside S10: the order statuses BrightCart accepts)

Run it on its own from the data_course folder:
    python 26.11.12_BrightCart_Daily_Sales_Pipeline_Mini_Project/02_scripts/silver.py 20261105
"""

# ===========================================================================
# Imports and Settings
# ===========================================================================

import sqlite3
import sys
 
import numpy as np
import pandas as pd
 
from config import get_paths, DATA_DIR

# ===========================================================================
# Configuration
# ===========================================================================

# N/A

# ===========================================================================
# Task S1: Customers - Remove spaces from all columns. 
# ===========================================================================
 
def trim_text_columns(bronze): 

    silver_trim_1 = bronze.copy()

    for column in silver_trim_1.columns:
        if pd.api.types.is_string_dtype(silver_trim_1[column]):
            silver_trim_1[column] = silver_trim_1[column].str.strip()
    return silver_trim_1
# Why not 'silver_trim_1[column] = silver_trim_1[column].str.strip().str.title()'? I don't want some columns, like status, to have this applied. 

# ===========================================================================
# Task S2: Customers - Capitalise ctites.
# ===========================================================================

def cap_cities_column(silver_trim_1):

    silver_cap_2 = silver_trim_1.copy()

    silver_cap_2["city"] = silver_cap_2["city"].str.title()
    return silver_cap_2
# Only the city column needs this fix.

# ===========================================================================
# Task S3: Customers - Fixing the data type per column.
# ===========================================================================


def data_type(silver_cap_2):

    silver_data_type_3 = silver_cap_2.copy()
    silver_data_type_3["customer_id"] = pd.to_numeric(silver_data_type_3["customer_id"], errors="coerce").astype("Int64")
    silver_data_type_3 = silver_data_type_3.replace("", np.nan)
    return(silver_data_type_3)
# Frist cleaning function just turns string into numberic. Thought it might cause problems to do this here but anyelle does it. Maybe for joins in SQL?
# Second cleaning line turns empty values into pandas form of na.

# ===========================================================================
# Task S4: Customers - Validation: Invalid = Missing customer_id, missing customer_name, missing city, duplicated customer_id
# ===========================================================================

def customer_validation(silver_data_type_3):

    silver_validation_4 = silver_data_type_3.copy()
    rejection_no_id_mask = silver_validation_4["customer_id"].isna()
    rejection_no_id = silver_validation_4[rejection_no_id_mask].copy()
    valid_no_id = silver_validation_4[~rejection_no_id_mask].copy()

    s_val_4_a = valid_no_id.copy()
    rejection_no_name_mask = s_val_4_a["customer_name"].isna()
    rejection_no_name = s_val_4_a[rejection_no_name_mask].copy()
    valid_no_name = s_val_4_a[~rejection_no_name_mask].copy()

    s_val_4_b = valid_no_name.copy()
    rejection_no_city_mask = s_val_4_b["city"].isna()
    rejection_no_city = s_val_4_b[rejection_no_city_mask].copy()
    valid_no_city = s_val_4_b[~rejection_no_city_mask].copy()

    s_val_4_c = valid_no_city.copy()
    rejection_duplicate_mask = s_val_4_c["customer_id"].duplicated(keep="first")
    rejection_duplicate = s_val_4_c[rejection_duplicate_mask].copy()
    valid_duplicate = s_val_4_c[~rejection_duplicate_mask].copy()
    # 26.10.03_Q17 Adds a new column and then does a filter, then I could drop the column. Don't need all those steps here.

    rejection_no_id["rejection_reason"] = "missing_customer_id"
    rejection_no_name["rejection_reason"] = "missing_customer_name"
    rejection_no_city["rejection_reason"] = "missing_city"
    rejection_duplicate["rejection_reason"] = "duplicate_customer_id"
    # Project asked for rejection reasosns to be added. 

    customers_valid = valid_duplicate.copy() 
    customers_invalid = pd.concat([rejection_no_id, rejection_no_name, rejection_no_city, rejection_duplicate])

    return(customers_valid, customers_invalid)

# ===========================================================================
# Task S5: Orders - Remove spaces from all columns. 
# ===========================================================================
 
def trim_text_orders(bronze): 

    silver_trim_5 = bronze.copy()

    for column in silver_trim_5.columns:
        if pd.api.types.is_string_dtype(silver_trim_5[column]):
            silver_trim_5[column] = silver_trim_5[column].str.strip()
    return silver_trim_5
# I didn't need to rewite this function, I could of just re-used it. 

# ===========================================================================
# Task S6: Orders - Make status capitals, so it matches VALID_STATUSES.
# ===========================================================================

def cap_status_column(silver_trim_5):

    silver_cap_6 = silver_trim_5.copy()

    silver_cap_6["status"] = silver_cap_6["status"].str.upper()
    return silver_cap_6

# ===========================================================================
# Task S7: Orders - Remove the £ sign. 
# ===========================================================================

def pound_sign_removal(silver_cap_6):

    silver_symbol_7 = silver_cap_6.copy()
    silver_symbol_7["unit_price_gbp"] = silver_symbol_7["unit_price_gbp"].str.replace("£", "", regex=False)
    return silver_symbol_7

# ===========================================================================
# Task S8: Orders - Convert order data formats, and missing into na. (Except Date.)
# ===========================================================================

def data_type_orders(silver_symbol_7):

    silver_type_8 = silver_symbol_7.copy()
    silver_type_8["order_id"] = pd.to_numeric(silver_type_8["order_id"], errors="coerce").astype("Int64")
    silver_type_8["customer_id"] = pd.to_numeric(silver_type_8["customer_id"], errors="coerce").astype("Int64")
    silver_type_8["quantity"] = pd.to_numeric(silver_type_8["quantity"], errors="coerce")
    silver_type_8["unit_price_gbp"] = pd.to_numeric(silver_type_8["unit_price_gbp"], errors="coerce")
    
    silver_type_8 = silver_type_8.replace("", np.nan)
    return(silver_type_8)

# ===========================================================================
# Task S9: Orders - Convert order_date format.
# ===========================================================================

def fix_order_date(silver_type_8):

    silver_date_9 = silver_type_8.copy()

    iso_dates = pd.to_datetime(silver_date_9["order_date"], format="%Y-%m-%d", errors="coerce")
    uk_dates = pd.to_datetime(silver_date_9["order_date"], format="%d/%m/%Y", errors="coerce")
    silver_date_9["order_date"] = iso_dates.fillna(uk_dates)
    return silver_date_9
# The first two lines of code turn the dates into the same format. Two different input formats into one output format.
# The third line is a pandas 'if' basically. Do the iso dates, and any that are blank, do the uk dates. Anything missing is already covered in lines 1 and 2.

# ===========================================================================
# Task S10: Orders - Validation: Invalid = na order_id, na customer_id, na price, na qty, na date, duplicate order_id, 
# <=0 price, <=0 qty, , invalid status, unknown customer.
# ===========================================================================

def validate_orders(silver_date_9, customers_valid):
    # See 's_val_10_d' for why it's not just 'def validate_orders(silver_date_9):'.

    silver_validation_10 = silver_date_9.copy()

    invalid_na_mask_10 = (((silver_validation_10["order_id"]).isna()) | (silver_validation_10["customer_id"].isna()) | (silver_validation_10["unit_price_gbp"].isna()) | (silver_validation_10["quantity"].isna()) | (silver_validation_10["order_date"].isna()))
    rejection_na_10 = silver_validation_10[invalid_na_mask_10].copy()
    valid_na_10 = silver_validation_10[~invalid_na_mask_10].copy()

    s_val_10_a = valid_na_10.copy()
    duplicate_mask_10 = s_val_10_a["order_id"].duplicated(keep="first")
    rejection_duplicate_10 = s_val_10_a[duplicate_mask_10].copy()
    valid_duplicate_10 = s_val_10_a[~duplicate_mask_10].copy()

    s_val_10_b = valid_duplicate_10.copy()
    less_than_or_equal_to_zero_mask = ((s_val_10_b["unit_price_gbp"] <= 0) | (s_val_10_b["quantity"] <= 0))
    # '((s_val_10_b[s_val_10_b["unit_price_gbp"]] <= 0) | (s_val_10_b[s_val_10_b["quantity"]] <= 0))', is wrong, because s_val_10_b["unit_price_gbp"] already gives the column. Wrapping it in s_val_10_b[...] again tries to find columns named after the prices, which errors. Making a mask = table["column"] <= 0. Using a mask = table[mask].
    rejection_zero = s_val_10_b[less_than_or_equal_to_zero_mask].copy()
    valid_zero = s_val_10_b[~less_than_or_equal_to_zero_mask].copy()

    s_val_10_c = valid_zero.copy()
    VALID_STATUSES = ["COMPLETED", "CANCELLED", "PENDING", "REFUNDED"]
    invalid_status_list_10 = []
    for status in s_val_10_c["status"]:
        if status in VALID_STATUSES:
            invalid_status_list_10.append(False)
        else:
            invalid_status_list_10.append(True)

    invalid_status_mask_10 = pd.Series(invalid_status_list_10, index=s_val_10_c.index)
    rejection_invalid_status_10 = s_val_10_c[invalid_status_mask_10].copy()
    valid_invalid_status_10 = s_val_10_c[~invalid_status_mask_10].copy()
    # Originally I tried a loop but it struggled with reading the whole table. Looping over a DataFrame gives the
    # column names, not the rows, so I loop over just the status column instead and build a True/False list,
    # then turn it into a mask with pd.Series(..., index=...) so it lines up with the right rows.
    # Option 1 (the shorter mask version, does the same thing):          
    # s_val_10_c = valid_zero.copy()
    # invalid_status_mask_10 = ~s_val_10_c["status"].isin(VALID_STATUSES)
    # rejection_invalid_status_10 = s_val_10_c[invalid_status_mask_10].copy()
    # valid_invalid_status_10 = s_val_10_c[~invalid_status_mask_10].copy()

    s_val_10_d = valid_invalid_status_10.copy()
    unknown_customer_mask_10 = ~s_val_10_d["customer_id"].isin(customers_valid["customer_id"])
    rejection_unknown_customer_10 = s_val_10_d[unknown_customer_mask_10].copy()
    valid_unknown_customer_10 = s_val_10_d[~unknown_customer_mask_10].copy()

    rejection_na_10["rejection_reason"] = "missing_value"
    rejection_duplicate_10["rejection_reason"] = "duplicate_order_id"
    rejection_zero["rejection_reason"] = "zero_or_negative_price_or_quantity"
    rejection_invalid_status_10["rejection_reason"] = "invalid_status"
    rejection_unknown_customer_10["rejection_reason"] = "unknown_customer"

    orders_valid = valid_unknown_customer_10.copy() 
    orders_invalid = pd.concat([rejection_na_10, rejection_duplicate_10, rejection_zero, rejection_invalid_status_10, rejection_unknown_customer_10])
    
    return(orders_valid, orders_invalid)

# ===========================================================================
# Task S11: Join valid orders to valid customers
# ===========================================================================

def join_orders_customers(orders_valid, customers_valid):

    silver_join_11 = orders_valid.merge(
        customers_valid[["customer_id", "customer_name", "city"]], on="customer_id", how="left", validate="many_to_one")
    # '[["customer_id", "customer_name", "city"]]' controls which columns from customers_valid come across. 
    return silver_join_11
# Anyelle had extra code in here to stop the script if a valid order had no city after the join.
# I handle that in S4 instead: customers with no city are rejected as missing_city, so their orders
# become unknown_customer in S10 and never reach the join.

# ===========================================================================
# Task S12: Row count checks.
# ===========================================================================
 
def silver_count_checks(run_date, bronze_customers, customers_valid, customers_invalid,
                        bronze_orders, orders_valid, orders_invalid, silver_join_11):
 
    if len(bronze_customers) != len(customers_valid) + len(customers_invalid):
        raise ValueError(f"[{run_date}] Customers do not reconcile: bronze {len(bronze_customers)}, "
                         f"valid {len(customers_valid)}, invalid {len(customers_invalid)}")
 
    if len(bronze_orders) != len(orders_valid) + len(orders_invalid):
        raise ValueError(f"[{run_date}] Orders do not reconcile: bronze {len(bronze_orders)}, "
                         f"valid {len(orders_valid)}, invalid {len(orders_invalid)}")
 
    if len(silver_join_11) != len(orders_valid):
        raise ValueError(f"[{run_date}] The join changed the number of orders: "
                         f"valid orders {len(orders_valid)}, after join {len(silver_join_11)}")
 
    print(f"[{run_date}] Customers: bronze {len(bronze_customers)} = valid {len(customers_valid)} + invalid {len(customers_invalid)}")
    print(f"[{run_date}] Orders: bronze {len(bronze_orders)} = valid {len(orders_valid)} + invalid {len(orders_invalid)}")
    print(f"[{run_date}] Join: {len(silver_join_11)} rows = {len(orders_valid)} valid orders")
 
# ===========================================================================
# Task S13: Save the valid and invalid files for the day.
# ===========================================================================
 
def write_silver_files(run_date, customers_valid, customers_invalid, silver_join_11, orders_invalid):
 
    paths = get_paths(run_date)
    paths["silver_valid"].mkdir(parents=True, exist_ok=True)
    paths["silver_rejected"].mkdir(parents=True, exist_ok=True)
 
    customers_valid.to_csv(paths["silver_valid"] / "customers.csv", index=False)
    silver_join_11.to_csv(paths["silver_valid"] / "orders.csv", index=False)
    customers_invalid.to_csv(paths["silver_rejected"] / "customers.csv", index=False)
    orders_invalid.to_csv(paths["silver_rejected"] / "orders.csv", index=False)
 
    print(f"[{run_date}] Wrote silver files to 03_silver/01_valid/{run_date}/ and 03_silver/02_rejected/{run_date}/")
    # Silver saves four files per day, not five, because the joined table is saved as the valid orders file.
    # The join only adds two columns from customers (customer_name and city) to the valid orders. It doesn't
    # add or remove any rows, so saving the unjoined valid orders as well would just be a duplicate of the
    # same data with two fewer columns. Gold reads this joined file, since it already has everything needed
    # for the city report.
 
# ===========================================================================
# Task S14: Load a silver DataFrame into the SQLite database (brightcart.db) for DBeaver.
# ===========================================================================

def load_to_sqlite(run_date, table_name, dataframe):

    sql_dataframe = dataframe.copy()
    sql_dataframe.insert(0, "run_date", run_date)

    (DATA_DIR / "07_sql_database").mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATA_DIR / "07_sql_database" / "brightcart.db")
    table_exists = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)).fetchone()
    if table_exists:
        connection.execute(f"DELETE FROM {table_name} WHERE run_date = ?", (run_date,))
    sql_dataframe.to_sql(table_name, connection, if_exists="append", index=False)
    connection.commit()
    connection.close()
    print(f"[{run_date}] Loaded {len(sql_dataframe)} rows into {table_name}")
    # ---------- CODE EXPLANATION ----------
    # Sends one DataFrame into a table in brightcart.db, a SQLite database file in 01_data/07_sql_database that DBeaver
    # can open. A run_date column is added first, so every day can live in one table and still be told apart.
    # mkdir makes the 07_sql_database folder if it isn't there yet. sqlite3.connect opens the database file, or creates
    # it if it isn't there yet. The sqlite_master query asks the database whether the table already exists. If it does,
    # DELETE removes that day's rows first, so a rerun replaces the day instead of duplicating it (the same as
    # DELETE WHERE run_date = '20261105' then INSERT). The ? is a placeholder that safely drops run_date into the SQL.
    # .to_sql(..., if_exists="append") then adds the day's rows, creating the table the first time. commit() saves the
    # changes and close() closes the database. The .db file is binary, so it is opened in DBeaver, not VS Code.
 
# ===========================================================================
# Task S15: The silver task: read bronze, run S1 to S14 in order. This is what Airflow calls.
# ===========================================================================
 
def run_silver(run_date):
 
    paths = get_paths(run_date)
    bronze_customers = pd.read_csv(paths["bronze"] / "customers.csv", dtype=str)
    bronze_orders = pd.read_csv(paths["bronze"] / "orders.csv", dtype=str)
 
    silver_trim_1 = trim_text_columns(bronze_customers)
    silver_cap_2 = cap_cities_column(silver_trim_1)
    silver_data_type_3 = data_type(silver_cap_2)
    customers_valid, customers_invalid = customer_validation(silver_data_type_3)
 
    silver_trim_5 = trim_text_orders(bronze_orders)
    silver_cap_6 = cap_status_column(silver_trim_5)
    silver_symbol_7 = pound_sign_removal(silver_cap_6)
    silver_type_8 = data_type_orders(silver_symbol_7)
    silver_date_9 = fix_order_date(silver_type_8)
    orders_valid, orders_invalid = validate_orders(silver_date_9, customers_valid)
 
    silver_join_11 = join_orders_customers(orders_valid, customers_valid)
 
    silver_count_checks(run_date, bronze_customers, customers_valid, customers_invalid,
                        bronze_orders, orders_valid, orders_invalid, silver_join_11)
 
    write_silver_files(run_date, customers_valid, customers_invalid, silver_join_11, orders_invalid)

    load_to_sqlite(run_date, "silver_customers_valid", customers_valid)
    load_to_sqlite(run_date, "silver_orders_valid", silver_join_11)
    load_to_sqlite(run_date, "silver_customers_rejected", customers_invalid)
    load_to_sqlite(run_date, "silver_orders_rejected", orders_invalid)
 
# ===========================================================================
# Task S16: Final run, only when I start this file myself in the terminal.
# ===========================================================================
 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please give a date, e.g. python 02_scripts/silver.py 20261105")
        sys.exit(1)
    try:
        run_silver(sys.argv[1])
    except Exception as Error:
        print(f"Error running run_silver: {Error}")
        raise
    print("(DB) Finished running.")