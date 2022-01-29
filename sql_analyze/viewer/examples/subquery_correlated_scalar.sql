SELECT col1,
       COALESCE ((SELECT Max(col2)
                  FROM   t1
                  WHERE  t1.col1 = t2.col1), 0) AS col2
FROM   t2;
