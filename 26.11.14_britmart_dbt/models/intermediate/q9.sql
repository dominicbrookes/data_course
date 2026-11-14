select
stg_orders.order_id,
stg_orders.customer_id,
stg_orders.order_date,
stg_customers.customer_name,
stg_customers.city,
stg_orders.amount_gbp,
stg_orders.status
from {{ref('stg_orders')}}
left join {{ref('stg_customers')}} on stg_orders.customer_id = stg_customers.customer_id