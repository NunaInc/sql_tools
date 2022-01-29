SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 EXCEPT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id > 50)
UNION
SELECT * 
FROM   emp 
WHERE  EXISTS (SELECT * 
                 FROM   dept 
                 WHERE  dept_id < 30 
                 INTERSECT 
                 SELECT * 
                 FROM   dept 
                 WHERE  dept_id >= 30 
                        AND dept_id <= 50);
