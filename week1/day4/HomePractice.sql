use ecommerce;


--- orders (id, customer_id, order_date, amount)
--- customers (id, name, region)
--- regions (region_id, region_name)

--- Write a CTE to calculate total order value per region.

with n as (
select c.id,c.name,c.region,r.region_name,o.amount 
from customers c
inner join 
orders o 
on c.id=o.customer_id
inner join regions r
on c.region=r.region_id)

select n.region,sum(n.amount) from n group by n.region;

--- Use another CTE to get average order value per customer.

select c.id,c.name,avg(o.amount) 
from customers c
inner join 
orders o 
on c.id=o.customer_id
group by c.id;


--- Create a materialized view that stores the monthly total sales per region.

--- Query the view to find top 3 regions in terms of monthly revenue.