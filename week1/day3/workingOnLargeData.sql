create database dummydata;
drop database dummydata;
use dummydata;
DROP TABLE IF EXISTS Projects;
DROP TABLE IF EXISTS Employees;

CREATE TABLE Employees (
    emp_id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    manager_id INT
);

CREATE TABLE Projects (
    proj_id INT PRIMARY KEY,
    proj_name VARCHAR(100),
    emp_id INT,
    FOREIGN KEY (emp_id) REFERENCES Employees(emp_id)
);

-- Indexes
CREATE INDEX idx_emp_dept ON Employees(department);
CREATE INDEX idx_emp_manager ON Employees(manager_id);
CREATE INDEX idx_proj_emp_id ON Projects(emp_id);


DELIMITER //
CREATE PROCEDURE populate_projects(p_start INT, p_end INT)
BEGIN
  DECLARE i INT;
  DECLARE rand_emp_id INT;
  SET i = p_start;

  WHILE i <= p_end DO
    SET rand_emp_id = (SELECT emp_id FROM Employees ORDER BY RAND() LIMIT 1);

    INSERT INTO Projects (
        proj_id, proj_name, emp_id
    )
    VALUES (
        i,
        CONCAT('Project_', i),
        rand_emp_id
    );

    SET i = i + 1;
  END WHILE;
END //
DELIMITER ;

-- CALL PROJECT POPULATION FOR 1,500 PROJECTS
CALL populate_projects(1, 1500);

-- Populate Employees with 10,000 rows
DELIMITER //
CREATE PROCEDURE populate_employees()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 1000 DO
    INSERT INTO Employees (
        emp_id, name, department, salary, manager_id
    )
    VALUES (
        i,
        CONCAT('Employee_', i),
        ELT(FLOOR(1 + RAND() * 5), 'Engineering', 'Sales', 'HR', 'Marketing', 'Finance'),
        ROUND(50000 + (RAND() * 50000), 2),
        FLOOR(1 + RAND() * 500)  -- Random manager_id from early employees
    );
    SET i = i + 1;
  END WHILE;
END //
DELIMITER ;

CALL populate_employees();


DROP PROCEDURE populate_employees;
DROP PROCEDURE populate_projects;

show tables;
explain select * from employees where department='hr';
select * from projects;