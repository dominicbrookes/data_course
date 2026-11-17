select
stg_customers.customer_id,
stg_customers.customer_name,
stg_customers.city
from {{ref('stg_customers')}}