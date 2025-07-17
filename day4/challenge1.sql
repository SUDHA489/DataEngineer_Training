use day4;

select * from customers;

select * from orders;

select customer_id ,name 
from (select c.customer_id,c.name,datediff(orderdate,curdate()) as daydifference 
from customers c inner join orders o 
on c.customer_id=o.customer_id) as new_table
group by customer_id having max(daydifference)>=-365;



--- using lag in query
select emp_id,department,salary,lag(salary) over(partition by department order by salary) as pre_salary from employees;

with cte as ( select department ,avg(salary) as avg_salary from employees group by department)
select department ,avg(salary) as avg_salary from employees group by department having avg_salary>55000;

select * from(select department ,avg(salary) as avg_salary from employees group by department) as new_table where avg_salary>55000;



