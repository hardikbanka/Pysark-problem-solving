# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 3

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
#PROBLEM STATEMENT: Write a sql query to return the footer values from input table, meaning all the last non null values from each field as shown in expected output.


	# INPUT				
	# ID	CAR	LENGTH	WIDTH	HEIGHT
	# 1	Hyundai Tucson	15	6	
	# 2				20
	# 3		12	8	15
	# 4	Toyota Rav4		15	
	# 5	Kia Sportage			18
					
					
	# EXPECTED OUTPUT			
	# CAR	LENGTH	WIDTH	HEIGHT
	# Kia Sportage	12	15	18


# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
# Import Required Libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

# Spark Session 
spark=SparkSession.Builder().appName("SQL-3").getOrCreate()

# Schema for Dataframe
schema=StructType([StructField("id",IntegerType()),
                   StructField("car",StringType()),
                   StructField("length",IntegerType()),
                   StructField("width",IntegerType()),
                   StructField("height",IntegerType())])
              
# Data for Dataframe
data = [
    (1, "Hyundai Tucson", 15, 6, None),
    (2, None, None, None, 20),
    (3, None, 12, 8, 15),
    (4, 'Toyota Rav4', None, 15, None),
    (5, 'Kia Sportage', None, None, 18)
]

# Create Dataframe and Temporary View
df=spark.createDataFrame(data,schema)
df.createOrReplaceTempView("cardetails")
display(df)



# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION - 1
# Creating a variable to store the last car value
lastcar=df.select(col("car")).where(col("car").isNotNull()).orderBy(col("id").desc()).head()

# Create a dataframe from the varibale
df_a=spark.createDataFrame([lastcar])

# Creating a variable to store the last length value
lastlength=df.select(col("length")).where(col("length").isNotNull()).orderBy(col("id").desc()).head()

# Create a dataframe from the varibale
df_b=spark.createDataFrame([lastlength])

# Creating a variable to store the last width value
lastwidth=df.select(col("width")).where(col("width").isNotNull()).orderBy(col("id").desc()).head()

# Create a dataframe from the varibale
df_c=spark.createDataFrame([lastwidth])

# Creating a variable to store the last height value
lastheight=df.select(col("height")).where(col("height").isNotNull()).orderBy(col("id").desc()).head()

# Create a dataframe from the varibale
df_d=spark.createDataFrame([lastheight])

# Cross join all the dataframes to create the final dataframe
df_final=df_a.crossJoin(df_b).crossJoin(df_c).crossJoin(df_d)
display(df_final)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION - 1
# MAGIC %sql
# MAGIC select * from (
# MAGIC Select car from cardetails where car is not null order by id desc limit 1) a 
# MAGIC cross join (Select length from cardetails where length is not null order by id desc limit 1) b
# MAGIC cross join 
# MAGIC (Select width from cardetails where width is not null order by id desc limit 1)c
# MAGIC cross join
# MAGIC (Select height from cardetails where height is not null order by id desc limit 1)d
