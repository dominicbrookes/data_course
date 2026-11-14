select
    trim(customer_name) as customer_name,
    customer_id,
    upper(city) as city
from {{source('britmart_raw', 'customers')}}