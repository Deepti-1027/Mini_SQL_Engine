# Mini_SQL_Engine
A mini SQL engine to parse and execute simple sql queries implemented in python

# Valid queries
  * Normal select queries
    * `select * from table1;`
    * `select a, b from table1;`
  * Aggregate functions like min, max, avg, sum, count
      * `select max(a), min(b) from table1;`
      * `select count(b) from table1;`
      * `select distinct(a) from table1;`
  * Conditional select with at most two conditions joined by and or or
      * `select a from table1 where a = 411;`
      * `select a, b from table1 where a = c or b = 5;`
  * Table join and aliasing
      * `select * from table1, table2;`
      * `select a, t1.b, c, t2.d from table1 as t1, table2 as t2 where t1.b = t2.b and t2.d >= 0;`

