USE Day4;

WITH ranked_employees AS (
    SELECT 
        emp_id,
        name,
        department,
        salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank,
        SUM(salary) OVER (PARTITION BY department) AS total_dept_salary
    FROM 
        employees
)
SELECT *
FROM ranked_employees
WHERE salary_rank <= 3;

