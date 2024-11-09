# Databricks notebook source
"""
Table: Employee

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
| department  | varchar |
| managerId   | int     |
+-------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table indicates the name of an employee, their department, and the id of their manager.
If managerId is null, then the employee does not have a manager.
No employee will be the manager of themself.
 

Write a solution to find managers with at least five direct reports.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+-----+-------+------------+-----------+
| id  | name  | department | managerId |
+-----+-------+------------+-----------+
| 101 | John  | A          | null      |
| 102 | Dan   | A          | 101       |
| 103 | James | A          | 101       |
| 104 | Amy   | A          | 101       |
| 105 | Anne  | A          | 101       |
| 106 | Ron   | B          | 101       |
+-----+-------+------------+-----------+
Output: 
+------+
| name |
+------+
| John |
+------+
"""

# COMMAND ----------

data = [[101, 'John', 'A', None], [102, 'Dan', 'A', 101], [103, 'James', 'A', 101], [104, 'Amy', 'A', 101], [105, 'Anne', 'A', 101], [106, 'Ron', 'B', 101]]

employee_df = spark.createDataFrame(data, ['id', 'name', 'department', 'managerId'])

employee_df.createTempView("Employee")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select name
# MAGIC from Employee
# MAGIC where id in(select managerId
# MAGIC     from Employee
# MAGIC     group by managerId
# MAGIC     having count(*) >4)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.name
# MAGIC FROM Employee e
# MAGIC JOIN (
# MAGIC     SELECT managerId
# MAGIC     FROM Employee
# MAGIC     WHERE managerId IS NOT NULL
# MAGIC     GROUP BY managerId
# MAGIC     HAVING COUNT(id) >= 5
# MAGIC ) AS managers_with_five_reports
# MAGIC ON e.id = managers_with_five_reports.managerId;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT manager.name
# MAGIC FROM Employee AS employee
# MAGIC JOIN Employee AS manager ON employee.managerId = manager.id
# MAGIC GROUP BY manager.id, manager.name
# MAGIC HAVING COUNT(employee.id) >= 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#self join
# (
#   employee_df.alias("emp").join(employee_df.alias("mgr"),
#                                     col("emp.id") == col("mgr.managerId"),
#                                     "inner"
#                               )
#               .groupBy(col("mgr.managerId"))
#               .agg(count("mgr.managerId").alias("manager_count"))
#               .filter(col("manager_count") > 4)
#               .select("name")
#               .show()
# )

from pyspark.sql.functions import col, count

# Self-join on employee_df to get pairs of managers and their direct reports
result = (
    employee_df.alias("emp")
    .join(
        employee_df.alias("mgr"),
        col("emp.managerId") == col("mgr.id"),
        "inner"
    )
    .groupBy(col("mgr.id"), col("mgr.name"))
    .agg(count("emp.id").alias("manager_count"))
    .filter(col("manager_count") > 4)
    .select("name")
)

result.show()


# COMMAND ----------


