# Project 1 - Spark query processing

## Goal of the project
Build a Spark application which provides an efficient parallel implementation for the following SQL query:

```sql
SELECT O_COUNT, COUNT(*) AS CUSTDIST
FROM (
    SELECT C_CUSTKEY, COUNT(O_ORDERKEY)
    FROM CUSTOMER left outer join ORDERS on C_CUSTKEY = O_CUSTKEY
    AND O_COMMENT not like '%special%requests%'
    GROUP BY C_CUSTKEY
) AS C_ORDERS (C_CUSTKEY, O_COUNT)
GROUP BY O_COUNT
```

where relations `CUSTOMER` and `ORDERS` have the following schema:
```sql
CUSTOMER(C_CUSTKEY     INTEGER NOT NULL,
         C_NAME        VARCHAR(25) NOT NULL,
         C_ADDRESS     VARCHAR(40) NOT NULL,
         C_NATIONKEY   INTEGER NOT NULL,
         C_PHONE       CHAR(15) NOT NULL,
         C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
         C_MKTSEGMENT  CHAR(10) NOT NULL,
         C_COMMENT     VARCHAR(117) NOT NULL),
         
ORDERS(O_ORDERKEY       INTEGER NOT NULL,
       O_CUSTKEY        INTEGER NOT NULL,
       O_ORDERSTATUS    CHAR(1) NOT NULL,
       O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
       O_ORDERDATE      DATE NOT NULL,
       O_ORDERPRIORITY  CHAR(15) NOT NULL, 
       O_CLERK          CHAR(15) NOT NULL,
       O_SHIPPRIORITY   INTEGER NOT NULL,
       O_COMMENT        VARCHAR(79) NOT NULL),
```
`C_CUSTKEY` and `O_ORDERKEY` are primary keys and `O_CUSTKEY` is a foreign key referencing `C_CUSTKEY`.

Note: For the innerquery, Customers with no orders, result in an `O_COUNT` of value 0.


Your application should provide 2 tasks:

1. an optimized implementation of the query that needs to scale / parallelize well on a large Skewed dataset (**50 GB**);
2. an optimized implementation of a variant of the query where the "left outer join" is replaced by an "inner join".

Your applications should take as arguments the input directory path, the output directory path and an integer 1 or 2, denoting which task to run.
Provide your tool as a jar file, i.e., Project2.jar. We must be able to run your tool, assuming the jar is in the current directory, by

`./bin/spark-submit --class "main.scala.Project2" --master yarn-cluster  --num executors 10  Project2.jar <input_path> <output_path> <task_flag = 1 or 2>`

## Challenges
There are two key challenges for implementing this project:
1. The 50gb dataset is much bigger than the last project and it is highly skewed: that is there is a highly skewed distribution of O_CUSTKEY (e.g. several keys that are repeated extensively). Thus, a naive implementation will not work well here. You will need to optimize your SPARK program to scale with the large skewed dataset. This requires understanding well what happens during partitioning.

2. Replacing the "left-outer-join" with an "inner join" opens up opportunities to rewrite the query to a more efficient one. Notice that integrity constraints are preserved, i.e., each customer has one or more orders.
