use day4;
select * from employees;

select department, salary, 
dense_rank() over (partition by department order by salary) as rownumber
from employees;

select emp_id, avg (salary) as avg_salary
from employees
group by department;

select department, avg(salary) 
over(partition by department) 
from employees;

--- task1
with rank_emp as(
select name,department, salary, 
dense_rank() over (partition by department order by salary asc) as rownumber
from employees)
select name, department, salary, rownumber
from rank_emp
where rownumber <=3;


--- task2
select * from (
 select name, department, salary, 
 dense_rank() over( partition by department order by salary asc)as ranking from employees) as new_table where ranking <= 3;
