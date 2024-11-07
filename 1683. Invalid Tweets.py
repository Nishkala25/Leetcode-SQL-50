# Databricks notebook source
"""
+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| tweet_id       | int     |
| content        | varchar |
+----------------+---------+
tweet_id is the primary key (column with unique values) for this table.
This table contains all the tweets in a social media app.
 

Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Tweets table:
+----------+-----------------------------------+
| tweet_id | content                           |
+----------+-----------------------------------+
| 1        | Let us Code                       |
| 2        | More than fifteen chars are here! |
+----------+-----------------------------------+
Output: 
+----------+
| tweet_id |
+----------+
| 2        |
+----------+
Explanation: 
Tweet 1 has length = 11. It is a valid tweet.
Tweet 2 has length = 33. It is an invalid tweet.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Tweets(tweet_id int, content varchar(50));
# MAGIC Truncate table Tweets;
# MAGIC insert into Tweets (tweet_id, content) values ('1', 'Let us Code');
# MAGIC insert into Tweets (tweet_id, content) values ('2', 'More than fifteen chars are here!');
# MAGIC
# MAGIC select * from Tweets;

# COMMAND ----------

# MAGIC %sql
# MAGIC select tweet_id
# MAGIC from Tweets
# MAGIC where length(content) > 15

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data = [
        (1, 'Let us Code'), 
        (2, 'More than fifteen chars are here!')
]

columns=['tweet_id', 'content']

tweets_df = spark.createDataFrame(data, columns)
tweets_df.show()

# COMMAND ----------

(tweets_df.select(col("tweet_id")) 
          .where(length(col("content")) > 15) 
          .show()
)


# COMMAND ----------

tweets_df.where(length(tweets_df["content"]) > 15) \
          .select(tweets_df["tweet_id"]) \
          .show()

# COMMAND ----------


