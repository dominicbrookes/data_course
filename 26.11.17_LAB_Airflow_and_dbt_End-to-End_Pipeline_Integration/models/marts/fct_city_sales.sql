select
int_completed_orders.city,
count(int_completed_orders.order_id) as order_count,
sum(int_completed_orders.amount_gbp) as revenue_gbp
from {{ref('int_completed_orders')}}
group by int_completed_orders.city