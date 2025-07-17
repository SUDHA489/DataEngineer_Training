use day4;

select * from sales;

select 
date_format(sale_date,'%Y-%m') as month,
sum(amount) as monthly_revenue,
sum(sum(amount)) over (order by date_format(sale_date,'%Y-%m'))as cummalative_revenue
from sales 
group by month
order by month;

