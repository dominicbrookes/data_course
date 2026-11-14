select
stg_orders.order_id,
stg_orders.customer_id,
cast(stg_orders.amount_gbp as decimal (10,2)) as amount_gbp,
stg_orders.order_date,
lower(stg_orders.status) as status,
trim(stg_customers.city) as city
from {{ref('stg_orders')}}
left join {{ref('stg_customers')}} on stg_orders.customer_id = stg_customers.customer_id
where lower(stg_orders.status) = 'complete'