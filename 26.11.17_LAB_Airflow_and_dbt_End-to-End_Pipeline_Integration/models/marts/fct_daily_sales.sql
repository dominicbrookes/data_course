select
q9.order_date,
q9.city,
count(q9.order_id) as order_count,
sum(q9.amount_gbp) as revenue_gbp
from {{ref('q9')}}
where q9.status = 'complete'
group by q9.order_date, q9.city

-- The only time this breaks is if you rename the file. In which case you need...
-- select
-- order_date,
-- city,
-- count(order_id) as order_count,
-- sum(amount_gbp) as revenue_gbp
-- from {{ref('q9')}}
-- where status = 'complete'
-- group by order_date, city