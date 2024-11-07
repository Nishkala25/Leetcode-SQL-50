# Databricks notebook source
"""
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
+---------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table contains the id and the name of an employee in a company.
 

Table: EmployeeUNI

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| unique_id     | int     |
+---------------+---------+
(id, unique_id) is the primary key (combination of columns with unique values) for this table.
Each row of this table contains the id and the corresponding unique id of an employee in the company.
 

Write a solution to show the unique ID of each user, If a user does not have a unique ID replace just show null.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employees table:
+----+----------+
| id | name     |
+----+----------+
| 1  | Alice    |
| 7  | Bob      |
| 11 | Meir     |
| 90 | Winston  |
| 3  | Jonathan |
+----+----------+
EmployeeUNI table:
+----+-----------+
| id | unique_id |
+----+-----------+
| 3  | 1         |
| 11 | 2         |
| 90 | 3         |
+----+-----------+
Output: 
+-----------+----------+
| unique_id | name     |
+-----------+----------+
| null      | Alice    |
| null      | Bob      |
| 2         | Meir     |
| 3         | Winston  |
| 1         | Jonathan |
+-----------+----------+
Explanation: 
Alice and Bob do not have a unique ID, We will show null instead.
The unique ID of Meir is 2.
The unique ID of Winston is 3.
The unique ID of Jonathan is 1.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Employees (id int, name varchar(20));
# MAGIC Create table If Not Exists EmployeeUNI (id int, unique_id int);
# MAGIC Truncate table Employees;
# MAGIC insert into Employees (id, name) values ('1', 'Alice');
# MAGIC insert into Employees (id, name) values ('7', 'Bob');
# MAGIC insert into Employees (id, name) values ('11', 'Meir');
# MAGIC insert into Employees (id, name) values ('90', 'Winston');
# MAGIC insert into Employees (id, name) values ('3', 'Jonathan');
# MAGIC Truncate table EmployeeUNI;
# MAGIC insert into EmployeeUNI (id, unique_id) values ('3', '1');
# MAGIC insert into EmployeeUNI (id, unique_id) values ('11', '2');
# MAGIC insert into EmployeeUNI (id, unique_id) values ('90', '3');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employees;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from EmployeeUNI;

# COMMAND ----------

# MAGIC %sql
# MAGIC select e2.unique_id, e1.name
# MAGIC from Employees e1
# MAGIC left join EmployeeUni e2
# MAGIC on e1.id = e2.id

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark
# MAGIC

# COMMAND ----------

data = [
  [1, 'Alice'], [7, 'Bob'], [11, 'Meir'], [90, 'Winston'], [3, 'Jonathan']]
employees_df = spark.createDataFrame(data, schema=['id', 'name'])
employees_df.display()

data1 = [[3, 1], [11, 2], [90, 3]]
columns=['id', 'unique_id']
employee_uni_df = spark.createDataFrame(data = data1, schema = columns)
employee_uni_df.display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------


(employees_df.join(employee_uni_df, on = "id", how = "left")
              .select("unique_id", "name")
              .show()
)

# COMMAND ----------

# Inner join using different column names

(employees_df.join(employee_uni_df, employees_df["id"]== employee_uni_df["id"], how = "left")
              .select(employee_uni_df["unique_id"], employees_df["name"])
              .show()
)



# COMMAND ----------


