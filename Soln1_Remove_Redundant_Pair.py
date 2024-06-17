# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 1

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
#  Problem Statement:
# - For pairs of brands in the same year (e.g. apple/samsung/2020 and samsung/apple/2020) 
#     - if custom1 = custom3 and custom2 = custom4 : then keep only one pair

# - For pairs of brands in the same year 
#     - if custom1 != custom3 OR custom2 != custom4 : then keep both pairs

# - For brands that do not have pairs in the same year : keep those rows as well

#Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,StructField
from pyspark.sql.functions import when, col, concat,row_number
from pyspark.sql.window import Window

# Spark Session
spark = SparkSession.builder.appName("SQL").getOrCreate()

#Schema for Dataframe
schema=StructType([StructField("Brand1",StringType()),
                   StructField("Brand2",StringType()),
                   StructField("Year",IntegerType()),
                   StructField("Custom1",IntegerType()),
                   StructField("Custom2",IntegerType()),
                   StructField("Custom3",IntegerType()),
                   StructField("Custom4",IntegerType()),
                   ])

# Data for Dataframe
data=[("apple","samsung",2020,1,2,1,2),
      ("samsung","apple",2020,1,2,1,2),
      ("apple","samsung",2021,1,2,5,3),
      ("samsung","apple",2021,5,3,1,2),
      ("google","",2020,5,9,0,0),
      ("oneplus","nothing",2020,5,9,6,3)]

# Create Dataframe and Temporary View
df_1=spark.createDataFrame(data=data,schema=schema)
df_1.createOrReplaceTempView("sql_1")

# Condition to create new ID column
df_1=df_1.withColumn("p1_id",when(col("Brand1")<col("Brand2"),concat(col("Brand1"),col("Brand2"),col("Year"))).otherwise(concat(col("Brand2"),col("Brand1"),col("Year"))))

# Window function Partitioning
w=Window.partitionBy("p1_id").orderBy("p1_id")
df_1=df_1.withColumn("rn",row_number().over(w))

# Filter to only required rows
df_1=df_1.filter((col("rn")==1) | ((col("custom1")!=col("custom3")) & (col("custom2")!=col("custom4"))))

display(df_1)

# COMMAND ----------

# DBTITLE 1,SQL- SOLUTION
# MAGIC %sql
# MAGIC with cte as (Select *,
# MAGIC        Case When brand1<brand2 Then concat(Brand1,Brand2,Year)
# MAGIC         else concat(Brand2,Brand1,Year) end as p1_id
# MAGIC from sql_1),
# MAGIC cte_rn as 
# MAGIC (select * ,row_number() over(partition by p1_id order by p1_id)as rn,case when custom1=custom3 and custom2=custom4 then 1
# MAGIC               else 0 end as flag from cte)
# MAGIC
# MAGIC select * from cte_rn 
# MAGIC where rn=1 or flag!=1
# MAGIC
