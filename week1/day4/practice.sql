use day4;


--- using union and union all
select name from customers 
union 
select name from suppliers;


--- using union all
select name from customers 
union all select name from suppliers;


--- using except

select name from customers 
except select name from suppliers;

--- creating views
select * from orders;

create view sales_summary as select customer_id,count(*) as total_count from orders group by customer_id; 

select * from sales_summary;


--- using merge command

select * from new_employees;

select * from employees;

merge into employees as target
using new_employees as source
on target.emp_id=source.emp_id
when matched then
update set salary = source.salary
when not matched then 
insert(emp_id,name,salary) 
values (source.emp_id,source.name,source.salary);


insert into employees(emp_id,name,department)
values (102,'ssn','cs') as new 
on duplicate key update 
name=values(name),
department=values(department);

select * from employees;


select * from accounts;

start transaction;
update accounts set balance=balance-200 where acc_id=1;
select sleep(30);
update accounts set balance=balance-100 where acc_id=2;
commit;











