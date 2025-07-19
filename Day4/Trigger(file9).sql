DELIMITER $$ 

CREATE TRIGGER log_salary_change 
AFTER UPDATE ON employees 
FOR EACH ROW 
BEGIN 
  INSERT INTO audit_log(emp_id, old_salary, new_salary) 
  VALUES (OLD.emp_id, OLD.salary, NEW.salary); 
END$$ 

DELIMITER ;

select * from audit_log;

update employees set salary=10 where emp_id=1;
