-- compare salery with prev example
select emp_id, department,salary,
	LAG(salary)OVER(PARTITION BY department ORDER BY salary)AS prev_salary
FROM employees;

-- calculate total salary per dept
select emp_id, department,salary,
	SUM(salary)OVER(PARTITION BY department) AS total_salary
from employees;