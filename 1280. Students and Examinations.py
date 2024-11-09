# Databricks notebook source
"""
Table: Students

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| student_id    | int     |
| student_name  | varchar |
+---------------+---------+
student_id is the primary key (column with unique values) for this table.
Each row of this table contains the ID and the name of one student in the school.
 

Table: Subjects

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| subject_name | varchar |
+--------------+---------+
subject_name is the primary key (column with unique values) for this table.
Each row of this table contains the name of one subject in the school.
 

Table: Examinations

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| student_id   | int     |
| subject_name | varchar |
+--------------+---------+
There is no primary key (column with unique values) for this table. It may contain duplicates.
Each student from the Students table takes every course from the Subjects table.
Each row of this table indicates that a student with ID student_id attended the exam of subject_name.
 

Write a solution to find the number of times each student attended each exam.

Return the result table ordered by student_id and subject_name.

The result format is in the following example.

 

Example 1:

Input: 
Students table:
+------------+--------------+
| student_id | student_name |
+------------+--------------+
| 1          | Alice        |
| 2          | Bob          |
| 13         | John         |
| 6          | Alex         |
+------------+--------------+
Subjects table:
+--------------+
| subject_name |
+--------------+
| Math         |
| Physics      |
| Programming  |
+--------------+
Examinations table:
+------------+--------------+
| student_id | subject_name |
+------------+--------------+
| 1          | Math         |
| 1          | Physics      |
| 1          | Programming  |
| 2          | Programming  |
| 1          | Physics      |
| 1          | Math         |
| 13         | Math         |
| 13         | Programming  |
| 13         | Physics      |
| 2          | Math         |
| 1          | Math         |
+------------+--------------+
Output: 
+------------+--------------+--------------+----------------+
| student_id | student_name | subject_name | attended_exams |
+------------+--------------+--------------+----------------+
| 1          | Alice        | Math         | 3              |
| 1          | Alice        | Physics      | 2              |
| 1          | Alice        | Programming  | 1              |
| 2          | Bob          | Math         | 1              |
| 2          | Bob          | Physics      | 0              |
| 2          | Bob          | Programming  | 1              |
| 6          | Alex         | Math         | 0              |
| 6          | Alex         | Physics      | 0              |
| 6          | Alex         | Programming  | 0              |
| 13         | John         | Math         | 1              |
| 13         | John         | Physics      | 1              |
| 13         | John         | Programming  | 1              |
+------------+--------------+--------------+----------------+
Explanation: 
The result table should contain all students and all subjects.
Alice attended the Math exam 3 times, the Physics exam 2 times, and the Programming exam 1 time.
Bob attended the Math exam 1 time, the Programming exam 1 time, and did not attend the Physics exam.
Alex did not attend any exams.
John attended the Math exam 1 time, the Physics exam 1 time, and the Programming exam 1 time.
"""

# COMMAND ----------

data = [[1, 'Alice'], [2, 'Bob'], [13, 'John'], [6, 'Alex']]
students_df = spark.createDataFrame(data, ['student_id', 'student_name'])

data = [['Math'], ['Physics'], ['Programming']]
subjects_df = spark.createDataFrame(data, ['subject_name'])

data = [[1, 'Math'], [1, 'Physics'], [1, 'Programming'], [2, 'Programming'], [1, 'Physics'], [1, 'Math'], [13, 'Math'], [13, 'Programming'], [13, 'Physics'], [2, 'Math'], [1, 'Math']]
examinations_df = spark.createDataFrame(data, ['student_id', 'subject_name'])

# COMMAND ----------

students_df.createTempView("Students")
subjects_df.createTempView("Subjects")
examinations_df.createTempView("Examinations")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     s.student_id,
# MAGIC     s.student_name,
# MAGIC     subj.subject_name,
# MAGIC     COUNT(e.student_id) AS attended_exams
# MAGIC FROM 
# MAGIC     Students s
# MAGIC CROSS JOIN 
# MAGIC     Subjects subj
# MAGIC LEFT JOIN 
# MAGIC     Examinations e 
# MAGIC ON 
# MAGIC     s.student_id = e.student_id 
# MAGIC     AND subj.subject_name = e.subject_name
# MAGIC GROUP BY 
# MAGIC     s.student_id, s.student_name, subj.subject_name
# MAGIC ORDER BY 
# MAGIC     s.student_id, subj.subject_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ExamCounts AS (
# MAGIC     SELECT 
# MAGIC         student_id, 
# MAGIC         subject_name, 
# MAGIC         COUNT(*) AS attended_exams
# MAGIC     FROM 
# MAGIC         Examinations
# MAGIC     GROUP BY 
# MAGIC         student_id, subject_name
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     s.student_id,
# MAGIC     s.student_name,
# MAGIC     subj.subject_name,
# MAGIC     COALESCE(ec.attended_exams, 0) AS attended_exams
# MAGIC FROM 
# MAGIC     Students s
# MAGIC CROSS JOIN 
# MAGIC     Subjects subj
# MAGIC LEFT JOIN 
# MAGIC     ExamCounts ec 
# MAGIC ON 
# MAGIC     s.student_id = ec.student_id 
# MAGIC     AND subj.subject_name = ec.subject_name
# MAGIC ORDER BY 
# MAGIC     s.student_id, subj.subject_name;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import * 

# COMMAND ----------

subjects_with_students = students_df.crossJoin(subjects_df)

attendence_count = (examinations_df.groupBy(col("student_id"), col("subject_name"))
                                    .agg(count('*').alias("attended_exams"))
                    )

joined_df = subjects_with_students.alias("s").join(attendence_count.alias("e"),
                                        (col("s.student_id")== col("e.student_id")) &
                                        (col("s.subject_name")== col("e.subject_name")),
                                        "left"
                                  )
result_df = joined_df.withColumn("attended_exams", coalesce(col("e.attended_exams"),lit(0)))\
                      .select(col("s.student_id"),col("s.student_name"), col("s.subject_name"), "attended_exams")\
                        .orderBy("student_id", "student_name", "subject_name")\
                      .show()

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming the data is loaded into DataFrames: `students`, `subjects`, `examinations`

# Step 1: Create a cross join of all students and subjects
students_with_subjects = students_df.crossJoin(subjects_df)

# Step 2: Count the number of times each student attended each exam
attendance_count = examinations_df.groupBy("student_id", "subject_name").agg(F.count("*").alias("attended_exams"))

# Step 3: Left join the students_with_subjects with the attendance_count using fully qualified column names
result = students_with_subjects.join(
    attendance_count,
    (students_with_subjects["student_id"] == attendance_count["student_id"]) & 
    (students_with_subjects["subject_name"] == attendance_count["subject_name"]),
    how='left'
)

# Step 4: Replace null values with 0 (students who didn't attend the exam)
result = result.withColumn("attended_exams", F.coalesce(result["attended_exams"], F.lit(0)))

# Step 5: Select the required columns and order by student_id and subject_name
final_result = result.select(
    students_with_subjects["student_id"], 
    students_with_subjects["student_name"], 
    students_with_subjects["subject_name"], 
    "attended_exams"
).orderBy("student_id", "subject_name")

# Show the result
final_result.show()


# COMMAND ----------


