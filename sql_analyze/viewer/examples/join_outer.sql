SELECT
  (SUM(COALESCE(t1.int_col1, t2.int_col0))),
     ((COALESCE(t1.int_col1, t2.int_col0)) * 2)
FROM t1
RIGHT JOIN t2
  ON (t2.int_col0) = (t1.int_col1)
GROUP BY GREATEST(COALESCE(t2.int_col1, 109), COALESCE(t1.int_col1, -449)),
         COALESCE(t1.int_col1, t2.int_col0)
HAVING (SUM(COALESCE(t1.int_col1, t2.int_col0)))
            > ((COALESCE(t1.int_col1, t2.int_col0)) * 2);
