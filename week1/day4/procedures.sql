use day4;

select * from employees;

--- creating procedure

delimiter $$
create procedure GetDetails()
begin 
select emp_id,name,salary 
from employees;
end$$

call GetDetails();


--- creating procedure using with inputs

DELIMITER $$
CREATE PROCEDURE GetEmployeeDetails(IN p_emp_id INT)
BEGIN
    SELECT emp_id, name, salary
    FROM employees
    WHERE emp_id = p_emp_id;
END$$

CALL GetEmployeeDetails(101);



delimiter $$
create procedure ChangeSalary()
begin 
update employees set salary=50000
where salary is null;
end$$

call ChangeSalary();