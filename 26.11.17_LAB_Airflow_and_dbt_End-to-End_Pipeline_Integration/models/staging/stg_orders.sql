select
    order_id,
    customer_id,
    amount_gbp,
    order_date,
    status,
    updated_at
from {{source('britmart_raw', 'orders')}}
where order_id is not null

-- ======================================================================
-- What happens when I run: dbt run --select stg_orders
-- (run from ~/data_course/26.11.14_britmart_dbt)
-- ======================================================================
-- 1. dbt finds the model. --select stg_orders means only build this one.
-- 2. dbt replaces the Jinja. It looks up britmart_raw -> orders in
--    sources.yml, sees schema: raw, and turns the source() line into a
--    real table name, roughly "dev"."raw"."orders".
--    The finished SQL is saved in target/compiled/.../stg_orders.sql
-- 3. No materialisation is set, so dbt uses the default: a VIEW.
--    It wraps my select in: create view main.stg_orders as ...
-- 4. dbt runs it on dev.duckdb. The view is created in the main schema,
--    next to dbt's two example models.
-- 5. dbt reports back, e.g.:
--    1 of 1 OK created sql view model main.stg_orders
--
-- Check: open the DuckDB UI -> dev -> main -> stg_orders.
-- The row with no order_id should be gone.
--
-- If it fails:
-- - "raw.orders does not exist"     -> the load into raw didn't happen
-- - "column amount_gbp not found"   -> the CSV uses a different column
--                                      name (check: DESCRIBE raw.orders;)
-- - lock error                      -> DuckDB or the UI still has
--                                      dev.duckdb open
-- ======================================================================