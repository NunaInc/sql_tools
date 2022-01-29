SELECT * 
FROM   bonus 
WHERE  NOT EXISTS (SELECT 1 
                   FROM   emp 
                   WHERE  emp.emp_name = bonus.emp_name 
                          AND EXISTS (SELECT Max(dept.dept_id) 
                                      FROM   dept 
                                      WHERE  emp.dept_id = dept.dept_id 
                                      GROUP  BY dept.dept_id));
