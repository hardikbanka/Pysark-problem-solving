# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 10

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# PROBLEM STATEMENT: 
# Write an sql query to get the output

# COMMAND ----------

# DBTITLE 1,CREATING DF & TABLES
# Import Required Libraries

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType,IntegerType,DateType
from pyspark.sql.functions import lit,col,max,min,when,to_date,count
from pyspark.sql.window import Window

# Creating spark session
spark=SparkSession.builder.appName("SQL-10").getOrCreate()

#Schema for the table
schema= StructType([StructField("client",StringType(),True),StructField("auto",StringType(),True),StructField("repair_date",IntegerType(),True),StructField("indicator",StringType(),True),StructField("value",StringType(),True)])

# Data
data = [("c1","a1",2022,"level","good"),("c1","a1",2022,"velocity","90"),("c1","a1",2023,"level","regular"),("c1","a1",2023,"velocity","80"),("c1","a1",2024,"level","wrong"),("c1","a1",2024,"velocity","70"),("c2","a1",2022,"level","good"),("c2","a1",2022,"velocity","90"),("c2","a1",2023,"level","wrong"),("c2","a1",2023,"velocity","50"),("c2","a2",2024,"level","good"),("c2","a2",2024,"velocity","80")]


#Create table and dataframe
df_auto=spark.createDataFrame(data,schema)
df_auto.createOrReplaceTempView("auto")



# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
a = (
    df_auto.filter(col("indicator") == "level")
    .withColumnRenamed("value", "level")
    .alias("a")
)
b = (
    df_auto.filter(col("indicator") == "velocity")
    .withColumnRenamed("value", "velocity")
    .alias("b")
)
df_join = a.join(
    b,
    (a["client"] == b["client"])
    & (a["auto"] == b["auto"])
    & (a["repair_date"] == b["repair_date"]),
    "inner",
).select("level", "velocity")

df_pivot = (
    df_join.groupBy("velocity")
    .pivot("level")
    .agg(count("level"))
    .orderBy("velocity")
)
display(df_pivot)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION
# MAGIC %sql
# MAGIC select *
# MAGIC from 
# MAGIC     (
# MAGIC         select v.value velocity, l.value level,count(1) as count
# MAGIC         from auto l
# MAGIC         join auto v on v.auto=l.auto and v.repair_date=l.repair_date and l.client=v.client
# MAGIC         where l.indicator='level'
# MAGIC         and v.indicator='velocity'
# MAGIC         group by v.value,l.value
# MAGIC     ) bq
# MAGIC pivot 
# MAGIC     (
# MAGIC         count(level)
# MAGIC         for level in ('good','wrong','regular')
# MAGIC     ) 
# MAGIC

# COMMAND ----------


