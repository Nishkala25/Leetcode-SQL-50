# Databricks notebook source
"""
Table: Weather

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the column with unique values for this table.
There are no different rows with the same recordDate.
This table contains information about the temperature on a certain day.
 

Write a solution to find all dates' id with higher temperatures compared to its previous dates (yesterday).

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Weather table:
+----+------------+-------------+
| id | recordDate | temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+
Output: 
+----+
| id |
+----+
| 2  |
| 4  |
+----+
Explanation: 
In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
In 2015-01-04, the temperature was higher than the previous day (20 -> 30).
"""

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Weather (id int, recordDate date, temperature int);
# MAGIC Truncate table Weather;
# MAGIC insert into Weather (id, recordDate, temperature) values ('1', '2015-01-01', '10');
# MAGIC insert into Weather (id, recordDate, temperature) values ('2', '2015-01-02', '25');
# MAGIC insert into Weather (id, recordDate, temperature) values ('3', '2015-01-03', '20');
# MAGIC insert into Weather (id, recordDate, temperature) values ('4', '2015-01-04', '30');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT w1.id
# MAGIC FROM Weather w1, Weather w2
# MAGIC WHERE DATEDIFF(w1.recordDate, w2.recordDate) = 1 AND w1.temperature > w2.temperature;

# COMMAND ----------

# MAGIC %sql
# MAGIC --in mysql
# MAGIC select a.id
# MAGIC from weather as a
# MAGIC left join (select date_add(recordDate, interval 1 day) as recordDate,   
# MAGIC                  temperature 
# MAGIC                  from weather
# MAGIC         ) as b 
# MAGIC on a.recordDate = b.recordDate
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --in spark sql
# MAGIC select a.id
# MAGIC from weather as a
# MAGIC left join (select date_add(recordDate, 1) as recordDate,   
# MAGIC                  temperature 
# MAGIC                  from weather
# MAGIC         ) as b 
# MAGIC on a.recordDate = b.recordDate
# MAGIC where a.temperature > b.temperature;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.id
# MAGIC FROM weather AS a
# MAGIC LEFT JOIN (
# MAGIC     SELECT DATE_ADD(recordDate, INTERVAL 1 DAY) AS recordDate,   
# MAGIC            temperature
# MAGIC     FROM weather
# MAGIC ) AS b 
# MAGIC ON a.recordDate = b.recordDate
# MAGIC WHERE a.temperature > b.temperature;
# MAGIC

# COMMAND ----------

spark.sql ("""
           select a.id
from weather as a
left join (select date_add(recordDate, 1) as recordDate,   
                 temperature 
                 from weather
        ) as b 
on a.recordDate = b.recordDate
where a.temperature > b.temperature;
           """)

# COMMAND ----------

data = [[1, '2015-01-01', 10], [2, '2015-01-02', 25], [3, '2015-01-03', 20], [4, '2015-01-04', 30]]
weather_df = spark.createDataFrame(data, schema=['id', 'recordDate', 'temperature'])
weather_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Convert 'recordDate' column to date type
weather_df = weather_df.withColumn('recordDate', F.col('recordDate').cast('date'))

# Join the weather DataFrame with itself on the previous day's date
result_df = weather_df.alias('a') \
    .join(weather_df.alias('b'), (F.date_add(F.col('a.recordDate'), 1) == F.col('b.recordDate')), 'left') \
    .filter(F.col('a.temperature') > F.col('b.temperature')) \
    .select('a.id')

# Show the result
result_df.show()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the weather DataFrame
weather_df = spark.read.table("weather")

# Define a window spec to partition by recordDate and order by recordDate
window_spec = Window.orderBy("recordDate")

# Use lag to get the previous day's temperature
weather_with_previous_temp = weather_df.withColumn("prev_temperature", F.lag("temperature", 1).over(window_spec))

# Filter rows where current temperature is greater than the previous day's temperature
result_df = weather_with_previous_temp.filter(F.col("temperature") > F.col("prev_temperature"))

# Select the id of the days where temperature is higher than the previous day
result_df = result_df.select("id")

# Show the result
result_df.show()


# COMMAND ----------


