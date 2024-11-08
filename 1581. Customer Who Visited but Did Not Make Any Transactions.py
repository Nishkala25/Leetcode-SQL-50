# Databricks notebook source
"""
Table: Visits

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| visit_id    | int     |
| customer_id | int     |
+-------------+---------+
visit_id is the column with unique values for this table.
This table contains information about the customers who visited the mall.
 

Table: Transactions

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| transaction_id | int     |
| visit_id       | int     |
| amount         | int     |
+----------------+---------+
transaction_id is column with unique values for this table.
This table contains information about the transactions made during the visit_id.
 

Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.

Return the result table sorted in any order.

The result format is in the following example.

 

Example 1:

Input: 
Visits
+----------+-------------+
| visit_id | customer_id |
+----------+-------------+
| 1        | 23          |
| 2        | 9           |
| 4        | 30          |
| 5        | 54          |
| 6        | 96          |
| 7        | 54          |
| 8        | 54          |
+----------+-------------+
Transactions
+----------------+----------+--------+
| transaction_id | visit_id | amount |
+----------------+----------+--------+
| 2              | 5        | 310    |
| 3              | 5        | 300    |
| 9              | 5        | 200    |
| 12             | 1        | 910    |
| 13             | 2        | 970    |
+----------------+----------+--------+
Output: 
+-------------+----------------+
| customer_id | count_no_trans |
+-------------+----------------+
| 54          | 2              |
| 30          | 1              |
| 96          | 1              |
+-------------+----------------+
Explanation: 
Customer with id = 23 visited the mall once and made one transaction during the visit with id = 12.
Customer with id = 9 visited the mall once and made one transaction during the visit with id = 13.
Customer with id = 30 visited the mall once and did not make any transactions.
Customer with id = 54 visited the mall three times. During 2 visits they did not make any transactions, and during one visit they made 3 transactions.
Customer with id = 96 visited the mall once and did not make any transactions.
As we can see, users with IDs 30 and 96 visited the mall one time without making any transactions. Also, user 54 visited the mall twice and did not make any transactions.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Visits(visit_id int, customer_id int);
# MAGIC Create table If Not Exists Transactions(transaction_id int, visit_id int, amount int);
# MAGIC Truncate table Visits;
# MAGIC insert into Visits (visit_id, customer_id) values ('1', '23');
# MAGIC insert into Visits (visit_id, customer_id) values ('2', '9');
# MAGIC insert into Visits (visit_id, customer_id) values ('4', '30');
# MAGIC insert into Visits (visit_id, customer_id) values ('5', '54');
# MAGIC insert into Visits (visit_id, customer_id) values ('6', '96');
# MAGIC insert into Visits (visit_id, customer_id) values ('7', '54');
# MAGIC insert into Visits (visit_id, customer_id) values ('8', '54');
# MAGIC Truncate table Transactions;
# MAGIC insert into Transactions (transaction_id, visit_id, amount) values ('2', '5', '310');
# MAGIC insert into Transactions (transaction_id, visit_id, amount) values ('3', '5', '300');
# MAGIC insert into Transactions (transaction_id, visit_id, amount) values ('9', '5', '200');
# MAGIC insert into Transactions (transaction_id, visit_id, amount) values ('12', '1', '910');
# MAGIC insert into Transactions (transaction_id, visit_id, amount) values ('13', '2', '970');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Visits;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC --answer
# MAGIC select t1.customer_id, count(customer_id) as count_no_trans
# MAGIC from Visits t1
# MAGIC left join Transactions t2
# MAGIC on t1.visit_id = t2.visit_id
# MAGIC where t2.transaction_id is null
# MAGIC group by customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

data = [(1, 23),
        (2, 9),
        (4, 30), 
        (5, 54), 
        (6, 96), 
        (7, 54), 
        (8, 54)
]

schema =['visit_id', 'customer_id']

visits_df  = spark.createDataFrame(data, schema )
visits_df.show()


data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
columns=['transaction_id', 'visit_id', 'amount']
transactions_df = spark.createDataFrame(data,columns)
transactions_df.show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

#answer
(
  visits_df.join(transactions_df, on = "visit_id", how = "left")
          .filter(col("transaction_id").isNull())
          .groupBy("customer_id")
          .agg(
              count(col("customer_id")).alias("count_no_trans")
          )
          .select("customer_id", "count_no_trans")
          .show()
)

# COMMAND ----------

# Step 1: Perform a left join on visit_id
joined_df = visits_df.join(transactions_df, on="visit_id", how="left")

# Step 2: Filter where transaction_id is NULL (indicating no transaction was made during that visit)
no_transaction_visits = joined_df.filter(col("transaction_id").isNull())

# Step 3: Group by customer_id and count the number of no-transaction visits
result_df = no_transaction_visits.groupBy("customer_id") \
    .agg(count("visit_id").alias("count_no_trans"))

# Show the result
result_df.show()

# COMMAND ----------


