# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 9

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# PROBLEM STATEMENT: 
# Write an sql query to merge products per customer for each day as shown in expected output.

# INPUT		
# CUSTOMER_ID	DATES	PRODUCT_ID
# 1	2/18/2024	101
# 1	2/18/2024	102
# 1	2/19/2024	101
# 1	2/19/2024	103
# 2	2/18/2024	104
# 2	2/18/2024	105
# 2	2/19/2024	101
# 2	2/19/2024	106


# OUTPUT	
# DATES	PRODUCTS
# 2/18/2024	101
# 2/18/2024	101,102
# 2/18/2024	102
# 2/18/2024	104
# 2/18/2024	104,105
# 2/18/2024	105
# 2/19/2024	101
# 2/19/2024	101,103
# 2/19/2024	101,106
# 2/19/2024	103
# 2/19/2024	106

# COMMAND ----------

# DBTITLE 1,CREATING DF & TABLES
#Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DateType
from pyspark.sql.functions import sum,min,col,lit,max,when,to_date,concat_ws,collect_list
from pyspark.sql.window import Window
# Spark Session Initialize
spark=SparkSession.builder.appName("SQL-9").getOrCreate()

#Schema for the table
schema=StructType([StructField("customer_id",IntegerType()),
                   StructField("dates",StringType()),
                   StructField("product_id",IntegerType())])

data = [(1, '2024-02-18', 101),
(1, '2024-02-18', 102),
(1, '2024-02-19', 101),
(1, '2024-02-19', 103),
(2, '2024-02-18', 104),
(2, '2024-02-18', 105),
(2, '2024-02-19', 101),
(2, '2024-02-19', 106) ]


#Createing dataframe and view
df_products= spark.createDataFrame(data,schema)
df_products=df_products.withColumn("dates",to_date(col("dates"),'yyyy-MM-dd'))

df_products.createOrReplaceTempView("products")

display(df_products)

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
df_products_1=df_products.groupBy("customer_id","dates").agg(concat_ws(",",collect_list("product_id")).alias("products")).orderBy("customer_id","dates")
display(df_products)

df_final=df_products.union(df_products_1)
display(df_final)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION
# MAGIC %sql
# MAGIC select 
# MAGIC     dates,
# MAGIC     cast(product_id as string)
# MAGIC from 
# MAGIC     products 
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select 
# MAGIC     dates,
# MAGIC     concat_ws(',', collect_list(cast(product_id as string))) as product_id
# MAGIC from 
# MAGIC     products 
# MAGIC group by 
# MAGIC     customer_id,dates
# MAGIC order by dates,product_id
