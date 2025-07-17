select * from employees;
USE Day4;

WITH employees AS (
    SELECT 
        emp_id,
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank,
        SUM(salary) OVER (PARTITION BY department) AS total_dept_salary
    FROM 
        employees
)
SELECT *
FROM employees
WHERE salary_rank <= 3;

