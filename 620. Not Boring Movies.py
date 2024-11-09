# Databricks notebook source
"""
Table: Cinema

+----------------+----------+
| Column Name    | Type     |
+----------------+----------+
| id             | int      |
| movie          | varchar  |
| description    | varchar  |
| rating         | float    |
+----------------+----------+
id is the primary key (column with unique values) for this table.
Each row contains information about the name of a movie, its genre, and its rating.
rating is a 2 decimal places float in the range [0, 10]
 

Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".

Return the result table ordered by rating in descending order.

The result format is in the following example.

 

Example 1:

Input: 
Cinema table:
+----+------------+-------------+--------+
| id | movie      | description | rating |
+----+------------+-------------+--------+
| 1  | War        | great 3D    | 8.9    |
| 2  | Science    | fiction     | 8.5    |
| 3  | irish      | boring      | 6.2    |
| 4  | Ice song   | Fantacy     | 8.6    |
| 5  | House card | Interesting | 9.1    |
+----+------------+-------------+--------+
Output: 
+----+------------+-------------+--------+
| id | movie      | description | rating |
+----+------------+-------------+--------+
| 5  | House card | Interesting | 9.1    |
| 1  | War        | great 3D    | 8.9    |
+----+------------+-------------+--------+
Explanation: 
We have three movies with odd-numbered IDs: 1, 3, and 5. The movie with ID = 3 is boring so we do not include it in the answer.
"""

# COMMAND ----------

data = [[1, 'War', 'great 3D', 8.9], [2, 'Science', 'fiction', 8.5], [3, 'irish', 'boring', 6.2], [4, 'Ice song', 'Fantacy', 8.6], [5, 'House card', 'Interesting', 9.1]]

cinema_df = spark.createDataFrame(data, ['id', 'movie', 'description', 'rating'])

cinema_df.createOrReplaceTempView("cinema")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from Cinema
# MAGIC where (id % 2) !=0 and description !="boring"
# MAGIC order by rating desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM Cinema 
# MAGIC WHERE description <> 'boring' AND id % 2 <> 0 
# MAGIC ORDER BY rating DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

from pyspark.sql.functions import col, desc

# COMMAND ----------

cinema_df.filter((col("description") != "boring") & ((col("id") % 2) != 0)).orderBy(desc("rating")).show()

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming `cinema_df` is the DataFrame that contains the Cinema table
cinema_df_filtered = cinema_df.filter(
    (F.col("id") % 2 != 0) &  # Only odd-numbered IDs
    (F.col("description") != "boring")  # Description is not "boring"
)

# Order by rating in descending order
cinema_df_sorted = cinema_df_filtered.orderBy(F.col("rating").desc())

# Show the result
cinema_df_sorted.select("id", "movie", "description", "rating").show()


# COMMAND ----------


