# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 4

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# Derive the below to expected output 
# INPUT						
# ID	NAME	LOCATION				
# 1						
# 2	David					
# 3		      London				
# 4						
# 5	David					
						
						
# EXPECTED OUTPUT - 1					
# ID	NAME	LOCATION		
# 1	David	London		


# EXPECTED OUTPUT - 2	
# ID	NAME	LOCATION
# 5	David	London

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
# Import Required Libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit,min,max
from pyspark.sql.types import StructType,StructField,StringType,IntegerType


# Spark Session 
spark=SparkSession.builder.appName("Q4").getOrCreate()

# Schema for Dataframe
schema=StructType([StructField("id",IntegerType(),True),
                   StructField("name",StringType(),True),
                   StructField("location",StringType(),True)])
              
# Data for Dataframe
data=[(1,None,None),
(2,'David',None),
(3,None,'London'),
(4,None,None),
(5,'David',None)]

# Create Dataframe and Temporary View
df=spark.createDataFrame(data,schema)
df.createOrReplaceTempView("people")
display(df)



# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION - 1
# OUTPUT 1
result1 = df.select(
    min("id").alias("id"),
    min("name").alias("name"),
    min("location").alias("location")
)

result1.show()


# COMMAND ----------

# DBTITLE 1,PYSPARK- SOLUTION 2
# OUTPUT 2
result2 = df.select(
    max("id").alias("id"),
    max("name").alias("name"),
    max("location").alias("location")
)

result2.show()


# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION - 1
# MAGIC %sql
# MAGIC
# MAGIC -- OUTPUT 1
# MAGIC select min(id) as id,min(name) as name, min(location) as location from people

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION - 2
# MAGIC %sql
# MAGIC
# MAGIC -- OUTPUT 2
# MAGIC select max(id) as id,min(name) as name, min(location) as location from people
