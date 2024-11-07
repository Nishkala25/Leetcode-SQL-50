# Databricks notebook source
"""
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| article_id    | int     |
| author_id     | int     |
| viewer_id     | int     |
| view_date     | date    |
+---------------+---------+
There is no primary key (column with unique values) for this table, the table may have duplicate rows.
Each row of this table indicates that some viewer viewed an article (written by some author) on some date. 
Note that equal author_id and viewer_id indicate the same person.
 

Write a solution to find all the authors that viewed at least one of their own articles.

Return the result table sorted by id in ascending order.

The result format is in the following example.

 

Example 1:

Input: 
Views table:
+------------+-----------+-----------+------------+
| article_id | author_id | viewer_id | view_date  |
+------------+-----------+-----------+------------+
| 1          | 3         | 5         | 2019-08-01 |
| 1          | 3         | 6         | 2019-08-02 |
| 2          | 7         | 7         | 2019-08-01 |
| 2          | 7         | 6         | 2019-08-02 |
| 4          | 7         | 1         | 2019-07-22 |
| 3          | 4         | 4         | 2019-07-21 |
| 3          | 4         | 4         | 2019-07-21 |
+------------+-----------+-----------+------------+
Output: 
+------+
| id   |
+------+
| 4    |
| 7    |
+------+"""

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------SQL-----------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Views (article_id int, author_id int, viewer_id int, view_date date)
# MAGIC ;
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('1', '3', '5', '2019-08-01');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('1', '3', '6', '2019-08-02');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('2', '7', '7', '2019-08-01');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('2', '7', '6', '2019-08-02');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('4', '7', '1', '2019-07-22');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('3', '4', '4', '2019-07-21');
# MAGIC insert into Views (article_id, author_id, viewer_id, view_date) values ('3', '4', '4', '2019-07-21');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `views`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(author_id) as id
# MAGIC from Views
# MAGIC where author_id = viewer_id
# MAGIC order by id;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark
# MAGIC

# COMMAND ----------

data = [[1, 3, 5, '2019-08-01'], [1, 3, 6, '2019-08-02'], [2, 7, 7, '2019-08-01'], [2, 7, 6, '2019-08-02'], [4, 7, 1, '2019-07-22'], [3, 4, 4, '2019-07-21'], [3, 4, 4, '2019-07-21']]


columns= ['article_id', 'author_id', 'viewer_id', 'view_date']

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


(df.select(col("author_id")).distinct()
    .filter(col("author_id") == col("viewer_id")) 
    .show()
)

# COMMAND ----------


