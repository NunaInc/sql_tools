SELECT *
FROM   t1 AS t1
WHERE  EXISTS (SELECT 1
               FROM   t2 AS t2
               WHERE  t1.col1 = t2.col1);
