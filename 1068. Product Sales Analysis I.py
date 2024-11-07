# Databricks notebook source
"""
+-------------+-------+
| Column Name | Type  |
+-------------+-------+
| sale_id     | int   |
| product_id  | int   |
| year        | int   |
| quantity    | int   |
| price       | int   |
+-------------+-------+
(sale_id, year) is the primary key (combination of columns with unique values) of this table.
product_id is a foreign key (reference column) to Product table.
Each row of this table shows a sale on the product product_id in a certain year.
Note that the price is per unit.
 

Table: Product

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| product_id   | int     |
| product_name | varchar |
+--------------+---------+
product_id is the primary key (column with unique values) of this table.
Each row of this table indicates the product name of each product.
 

Write a solution to report the product_name, year, and price for each sale_id in the Sales table.

Return the resulting table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Sales table:
+---------+------------+------+----------+-------+
| sale_id | product_id | year | quantity | price |
+---------+------------+------+----------+-------+ 
| 1       | 100        | 2008 | 10       | 5000  |
| 2       | 100        | 2009 | 12       | 5000  |
| 7       | 200        | 2011 | 15       | 9000  |
+---------+------------+------+----------+-------+
Product table:
+------------+--------------+
| product_id | product_name |
+------------+--------------+
| 100        | Nokia        |
| 200        | Apple        |
| 300        | Samsung      |
+------------+--------------+
Output: 
+--------------+-------+-------+
| product_name | year  | price |
+--------------+-------+-------+
| Nokia        | 2008  | 5000  |
| Nokia        | 2009  | 5000  |
| Apple        | 2011  | 9000  |
+--------------+-------+-------+
Explanation: 
From sale_id = 1, we can conclude that Nokia was sold for 5000 in the year 2008.
From sale_id = 2, we can conclude that Nokia was sold for 5000 in the year 2009.
From sale_id = 7, we can conclude that Apple was sold for 9000 in the year 2011.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists Sales (sale_id int, product_id int, year int, quantity int, price int);
# MAGIC Create table If Not Exists Product (product_id int, product_name varchar(10));
# MAGIC Truncate table Sales;
# MAGIC insert into Sales (sale_id, product_id, year, quantity, price) values ('1', '100', '2008', '10', '5000');
# MAGIC insert into Sales (sale_id, product_id, year, quantity, price) values ('2', '100', '2009', '12', '5000');
# MAGIC insert into Sales (sale_id, product_id, year, quantity, price) values ('7', '200', '2011', '15', '9000');
# MAGIC Truncate table Product;
# MAGIC insert into Product (product_id, product_name) values ('100', 'Nokia');
# MAGIC insert into Product (product_id, product_name) values ('200', 'Apple');
# MAGIC insert into Product (product_id, product_name) values ('300', 'Samsung');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Product;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Answer
# MAGIC select t2.product_name, t1.year, t1.price
# MAGIC from Sales t1
# MAGIC left join Product t2
# MAGIC on t1.product_id = t2.product_id

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

sales_df = spark.sql("select * from Sales")
sales_df.display()

product_df = spark.sql("select * from Product")
product_df.display()

# COMMAND ----------

#Answer
(
  sales_df.join(product_df, sales_df["product_id"] == product_df["product_id"],"left")
          .select(product_df["product_name"], sales_df["year"], sales_df["price"])
          .show()
)

# COMMAND ----------

(
  sales_df.join(product_df, on = "product_id", how = "left")
          .select(product_df["product_name"], sales_df["year"], sales_df["price"])
          .show()
)

# COMMAND ----------


