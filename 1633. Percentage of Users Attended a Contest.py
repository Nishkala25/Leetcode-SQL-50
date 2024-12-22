# Databricks notebook source
"""
Table: Users

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| user_id     | int     |
| user_name   | varchar |
+-------------+---------+
user_id is the primary key (column with unique values) for this table.
Each row of this table contains the name and the id of a user.
 

Table: Register

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| contest_id  | int     |
| user_id     | int     |
+-------------+---------+
(contest_id, user_id) is the primary key (combination of columns with unique values) for this table.
Each row of this table contains the id of a user and the contest they registered into.
 

Write a solution to find the percentage of the users registered in each contest rounded to two decimals.

Return the result table ordered by percentage in descending order. In case of a tie, order it by contest_id in ascending order.

The result format is in the following example.

 

Example 1:

Input: 
Users table:
+---------+-----------+
| user_id | user_name |
+---------+-----------+
| 6       | Alice     |
| 2       | Bob       |
| 7       | Alex      |
+---------+-----------+
Register table:
+------------+---------+
| contest_id | user_id |
+------------+---------+
| 215        | 6       |
| 209        | 2       |
| 208        | 2       |
| 210        | 6       |
| 208        | 6       |
| 209        | 7       |
| 209        | 6       |
| 215        | 7       |
| 208        | 7       |
| 210        | 2       |
| 207        | 2       |
| 210        | 7       |
+------------+---------+
Output: 
+------------+------------+
| contest_id | percentage |
+------------+------------+
| 208        | 100.0      |
| 209        | 100.0      |
| 210        | 100.0      |
| 215        | 66.67      |
| 207        | 33.33      |
+------------+------------+
Explanation: 
All the users registered in contests 208, 209, and 210. The percentage is 100% and we sort them in the answer table by contest_id in ascending order.
Alice and Alex registered in contest 215 and the percentage is ((2/3) * 100) = 66.67%
Bob registered in contest 207 and the percentage is ((1/3) * 100) = 33.33%
"""

# COMMAND ----------

data = [[6, 'Alice'], [2, 'Bob'], [7, 'Alex']]
users_df = spark.createDataFrame(data, ['user_id', 'user_name'])

data = [[215, 6], [209, 2], [208, 2], [210, 6], [208, 6], [209, 7], [209, 6], [215, 7], [208, 7], [210, 2], [207, 2], [210, 7]]
register_df = spark.createDataFrame(data, ['contest_id', 'user_id'])

users_df.createOrReplaceTempView("users")
register_df.createOrReplaceTempView("register")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select r.contest_id, 
# MAGIC     round(
# MAGIC         count(r.user_id)/(select count(user_id) from Users)*100 
# MAGIC     , 2)as percentage
# MAGIC from Users u
# MAGIC join Register r
# MAGIC on u.user_id = r.user_id
# MAGIC group by r.contest_id
# MAGIC order by percentage desc, r.contest_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --better solution
# MAGIC select contest_id, 
# MAGIC     round(count(user_id) * 100 /(select count(user_id) from Users) ,2) as percentage
# MAGIC from  Register
# MAGIC group by contest_id
# MAGIC order by percentage desc,contest_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark

# COMMAND ----------

from pyspark.sql.functions import col, count, round

# Calculate total users
total_users = users_df.count()

# Calculate registration percentage per contest
result_df = register_df.groupBy("contest_id") \
    .agg((count("user_id") / total_users * 100).alias("percentage")) \
    .withColumn("percentage", round(col("percentage"), 2)) \
    .orderBy(col("percentage").desc(), col("contest_id"))

# Show result
result_df.show()


# COMMAND ----------


