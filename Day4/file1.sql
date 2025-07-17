select * from employees;
select emp_id, department, salary,
ROW_NUMBER() OVER (PARTITION BY department Order by Salary Desc) AS Salary_RANK
from employees;

select * from employees;
with dept_avg As(select department, Avg(salary) as avg_salary from employees group by department)
select * from dept_avg where avg_salary>50000;

SELECT *
FROM (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
) AS avg_dept;

select name from customers
union
select name from suppliers;

select name as unionname from(select name from customers
union
select name from suppliers) as final;

select name from customers
except
select name from suppliers;

create view sales_summary as
select region,sum(amount) as total_sales
from orders
group by region;

select * from employees;

INSERT INTO employees (emp_id, name, department)
VALUES (101, 'Alice', 'IT'), (1, 'Akash', 'Sales')
ON DUPLICATE KEY UPDATE
name = VALUES(name),
department = VALUES(department);

select * from employees;