use day4;

select * from audit_log;


--- created a trigger on employees table 

drop trigger before_salary_update;  --- drop if exists


DELIMITER $$
CREATE TRIGGER before_salary_update
before UPDATE ON employees
FOR EACH ROW
BEGIN
    IF OLD.salary <> NEW.salary THEN
        INSERT INTO audit_log(emp_id, old_salary, new_salary, changed_at)
        VALUES (OLD.emp_id, OLD.salary, NEW.salary, NOW());
    END IF;
END$$
DELIMITER ;

select * from employees;
UPDATE employees SET salary = 65000 WHERE emp_id = 101;
select * from audit_log;
