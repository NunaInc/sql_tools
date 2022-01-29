SELECT * 
FROM   dept 
WHERE  EXISTS (SELECT dept_id, 
                      count(emp.dept_id)
               FROM   emp 
               WHERE  dept.dept_id = dept_id 
               GROUP  BY dept_id 
               HAVING EXISTS (SELECT 1 
                              FROM   bonus 
                              WHERE  ( bonus_amt > min(emp.salary) 
                                       AND count(emp.dept_id) > 1 )));
