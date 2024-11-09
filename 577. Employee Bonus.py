# Databricks notebook source
"""
Table: Employee

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| empId       | int     |
| name        | varchar |
| supervisor  | int     |
| salary      | int     |
+-------------+---------+
empId is the column with unique values for this table.
Each row of this table indicates the name and the ID of an employee in addition to their salary and the id of their manager.
 

Table: Bonus

+-------------+------+
| Column Name | Type |
+-------------+------+
| empId       | int  |
| bonus       | int  |
+-------------+------+
empId is the column of unique values for this table.
empId is a foreign key (reference column) to empId from the Employee table.
Each row of this table contains the id of an employee and their respective bonus.
 

Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+-------+--------+------------+--------+
| empId | name   | supervisor | salary |
+-------+--------+------------+--------+
| 3     | Brad   | null       | 4000   |
| 1     | John   | 3          | 1000   |
| 2     | Dan    | 3          | 2000   |
| 4     | Thomas | 3          | 4000   |
+-------+--------+------------+--------+
Bonus table:
+-------+-------+
| empId | bonus |
+-------+-------+
| 2     | 500   |
| 4     | 2000  |
+-------+-------+
Output: 
+------+-------+
| name | bonus |
+------+-------+
| Brad | null  |
| John | null  |
| Dan  | 500   |
+------+-------+
"""

# COMMAND ----------

data = [[3, 'Brad', None, 4000], [1, 'John', 3, 1000], [2, 'Dan', 3, 2000], [4, 'Thomas', 3, 4000]]
employee_df = spark.createDataFrame(data, schema=['empId', 'name', 'supervisor', 'salary'])

data = [[2, 500], [4, 2000]]
bonus_df = spark.createDataFrame(data, schema=['empId', 'bonus'])

# COMMAND ----------

bonus_df.createTempView("Bonus")
employee_df.createTempView("Employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.name, t2.bonus
# MAGIC from Employee t1
# MAGIC left join Bonus t2
# MAGIC on t1.empId = t2.empId
# MAGIC where t2.bonus < 1000 or t2.bonus is null;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

#t1 = spark.read.table("Employee") 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

(employee_df.join(bonus_df, "empId", "left")
            .filter((col("bonus") < 1000) | (col("bonus").isNull()))
            .select("name", "bonus")
            .show()
)

# COMMAND ----------

#chatgpt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Assuming Spark session is created as follows
spark = SparkSession.builder.appName("EmployeeBonus").getOrCreate()

# Sample data for Employee table
employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

# Sample data for Bonus table
bonus_data = [
    (2, 500),
    (4, 2000)
]

# Creating DataFrames
employee_columns = ["empId", "name", "supervisor", "salary"]
bonus_columns = ["empId", "bonus"]

employee_df = spark.createDataFrame(employee_data, employee_columns)
bonus_df = spark.createDataFrame(bonus_data, bonus_columns)

# Left join Employee with Bonus on empId
employee_bonus_df = employee_df.join(bonus_df, on="empId", how="left")

# Filter for employees with bonus < 1000 or no bonus (bonus is null)
result_df = employee_bonus_df.filter((col("bonus") < 1000) | col("bonus").isNull()).select("name", "bonus")

# Display result
result_df.show()


# COMMAND ----------


