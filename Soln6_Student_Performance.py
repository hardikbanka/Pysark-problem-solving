# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 6

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# You are given a table having the marks of one student in every test. 
# You have to output the tests in which the student has improved his performance. 
# For a student to improve his performance he has to score more than the previous test.
# Provide 2 solutions, one including the first test score and second excluding it

	
INPUT	
TEST_ID	MARKS
100	55
101	55
102	60
103	58
104	40
105	50

			
OUTPUT - 1	
TEST_ID	MARKS
100	55
102	60
105	50

OUTPUT - 2	
TEST_ID	MARKS
102	60
105	50





# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
#Importing Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit,max,min,lag
from pyspark.sql.types import StructType,StructField,IntegerType
from pyspark.sql.window import Window

# Spark Session Initialize
spark=SparkSession.builder.appName("Student").getOrCreate()

#Schema for the test mark table
Schema=StructType([StructField("Test_id",IntegerType()),
                   StructField("Marks",IntegerType())])

# Data for the table
data =[(100,55),(101,55),(102,60),(103,58),(104,40),(105,50)]

#Create DataFrame
df_student=spark.createDataFrame(data,Schema)
display(df_student)

# Create a temporary view from the dataframe
df_student.createOrReplaceTempView("student")

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION - OUTPUT 1
# Used LAG window function
w=Window.orderBy("test_id")
df_student=df_student.withColumn("prev_marks",lag(col("marks"),1).over(w))

# Filter the dataframe only to get where student performance and increased and also to have first test
df_student_1=df_student.filter((col("marks") > col("prev_marks")) | ((col("prev_marks").isNull())))
display(df_student_1)

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION - OUTPUT 2
# Used LAG window function
w=Window.orderBy("test_id")
df_student=df_student.withColumn("prev_marks",lag(col("marks"),1).over(w))

# Filter the dataframe only to get where student performance and increased
df_student_2=df_student.filter((col("marks") > col("prev_marks")))
display(df_student_2)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION - OUTPUT 1
# MAGIC %sql
# MAGIC WITH prev_mark AS (
# MAGIC   SELECT *, LAG(marks) OVER (ORDER BY test_id) AS prev_marks
# MAGIC   FROM student
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM prev_mark where marks>prev_marks or prev_marks is null

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION - OUTPUT 2
# MAGIC %sql
# MAGIC WITH prev_mark AS (
# MAGIC   SELECT *, LAG(marks) OVER (ORDER BY test_id) AS prev_marks
# MAGIC   FROM student
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM prev_mark where marks>prev_marks
