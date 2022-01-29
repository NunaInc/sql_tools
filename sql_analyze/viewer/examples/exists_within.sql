SELECT * FROM bonus WHERE NOT EXISTS 
( 
       SELECT * 
       FROM   emp 
       WHERE  emp.emp_name = emp_name 
       AND    bonus_amt > emp.salary) 
AND 
emp_name IN 
( 
       SELECT emp_name 
       FROM   emp 
       WHERE  bonus_amt < emp.salary);
