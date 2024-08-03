# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 7

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# PROBLEM STATEMENT:
# In the given input table DAY_INDICATOR field indicates the day of the week with the first character being Monday, followed by Tuesday and so on.
# Write a query to filter the dates column to showcase only those days where day_indicator character for that day of the week is 1

	
# INPUT		
# PRODUCT_ID	DAY_INDICATOR	DATES
# AP755	1010101	3/4/2024
# AP755	1010101	3/5/2024
# AP755	1010101	3/6/2024
# AP755	1010101	3/7/2024
# AP755	1010101	3/8/2024
# AP755	1010101	3/9/2024
# AP755	1010101	3/10/2024
# XQ802	1000110	3/4/2024
# XQ802	1000110	3/5/2024
# XQ802	1000110	3/6/2024
# XQ802	1000110	3/7/2024
# XQ802	1000110	3/8/2024
# XQ802	1000110	3/9/2024
# XQ802	1000110	3/10/2024


# OUTPUT		
# PRODUCT_ID	DAY_INDICATOR	DATES
# AP755	1010101	3/4/2024
# AP755	1010101	3/6/2024
# AP755	1010101	3/8/2024
# AP755	1010101	3/10/2024
# XQ802	1000110	3/4/2024
# XQ802	1000110	3/8/2024
# XQ802	1000110	3/9/2024


# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
#Importing Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,DateType,StringType
from pyspark.sql.functions import col,when,max,min,lit,to_date,dayofweek,substring,expr
from pyspark.sql.window import Window

# Spark Session Initialize
spark=SparkSession.builder.appName("SQL-7").getOrCreate()

#Schema for the test mark table
Schema=StructType([StructField("product_id",StringType()),
                   StructField("day_indicator",StringType()),
                   StructField("Dates",StringType())])

# Data for the table
data =[('AP755', '1010101', '04-Mar-2024'),
('AP755', '1010101', '05-Mar-2024'),
('AP755', '1010101', '06-Mar-2024'),
('AP755', '1010101', '07-Mar-2024'),
('AP755', '1010101', '08-Mar-2024'),
('AP755', '1010101', '09-Mar-2024'),
('AP755', '1010101', '10-Mar-2024'),
('XQ802', '1000110', '04-Mar-2024'),
('XQ802', '1000110', '05-Mar-2024'),
('XQ802', '1000110', '06-Mar-2024'),
('XQ802', '1000110', '07-Mar-2024'),
('XQ802', '1000110', '08-Mar-2024'),
('XQ802', '1000110', '09-Mar-2024'),
('XQ802', '1000110', '10-Mar-2024')]

#Create DataFrame
df_product=spark.createDataFrame(data,Schema)
display(df_product)

# Create a temporary view from the dataframe
df_product.createOrReplaceTempView("product")

# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
df_product = df_product.withColumn("date_column", to_date(col("dates"), 'dd-MMM-yyyy'))
df_product = df_product.withColumn("day_index", dayofweek(col("date_column")))
df_product = df_product.withColumn(
    "flag", 
    when(expr("substring(day_indicator, day_index, 1)") == '1', '1').otherwise('0')
)

df_product=df_product.filter(col("flag")=='1').select("product_id","day_indicator","dates")
display(df_product)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION
# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     dayofweek(to_date(dates, 'dd-MMM-yyyy')) as day_index,
# MAGIC     CASE
# MAGIC       WHEN SUBSTRING(
# MAGIC         day_indicator,
# MAGIC         dayofweek(to_date(dates, 'dd-MMM-yyyy')),
# MAGIC         1
# MAGIC       ) = '1' THEN '1'
# MAGIC       ELSE '0'
# MAGIC     END AS flag
# MAGIC   FROM
# MAGIC     product
# MAGIC )
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   day_indicator,
# MAGIC   dates
# MAGIC FROM
# MAGIC   cte
# MAGIC where
# MAGIC   flag = 1
