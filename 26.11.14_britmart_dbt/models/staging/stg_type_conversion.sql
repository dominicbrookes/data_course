-- try_cast instead of cast. If any amount_text value isn’t a valid number (like "abc" or "£4.25"), cast makes the whole model fail, while try_cast puts null for that row and carries on. Either is fine for the lab; cast is what the question expects. 
select
order_id,
order_date,
try_cast(amount_gbp as decimal(10,2)) as amount_gbp
from {{source('britmart_raw','orders')}}
