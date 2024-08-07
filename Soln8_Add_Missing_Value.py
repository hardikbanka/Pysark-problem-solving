# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 8

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# PROBLEM STATEMENT: 
# In the given input table, there are rows with missing JOB_ROLE values. Write a query to fill in those blank fields with appropriate values.
# Assume row_id is always in sequence and job_role field is populated only for the first skill.
# Provide two different solutions to the problem.

	
# INPUT		
# ROW_ID	JOB_ROLE	SKILLS
# 1	Data Engineer	SQL
# 2		Python
# 3		AWS
# 4		Snowflake
# 5		Apache Spark
# 6	Web Developer	Java
# 7		HTML
# 8		CSS
# 9	Data Scientist	Python
# 10		Machine Learning
# 11		Deep Learning
# 12		Tableau



# OUTPUT		
# ROW_ID	JOB_ROLE	SKILLS
# 1	Data Engineer	SQL
# 2	Data Engineer	Python
# 3	Data Engineer	AWS
# 4	Data Engineer	Snowflake
# 5	Data Engineer	Apache Spark
# 6	Web Developer	Java
# 7	Web Developer	HTML
# 8	Web Developer	CSS
# 9	Data Scientist	Python
# 10	Data Scientist	Machine Learning
# 11	Data Scientist	Deep Learning
# 12	Data Scientist	Tableau



# COMMAND ----------

# DBTITLE 1,CREATING DF & TABLES
#Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
from pyspark.sql.functions import col,when,max,min,lit,substring,sum,first
from pyspark.sql.window import Window

# Spark Session Initialize
spark=SparkSession.builder.appName("SQL-8").getOrCreate()

#Schema for the table
schema=StructType([StructField("row_id",IntegerType(),False),
                   StructField("job_role",StringType()),
                   StructField("skills",StringType())])


data=[(1, 'Data Engineer', 'SQL'),
(2, None, 'Python'),
(3, None, 'AWS'),
(4, None, 'Snowflake'),
(5, None, 'Apache Spark'),
(6, 'Web Developer', 'Java'),
(7, None, 'HTML'),
(8, None, 'CSS'),
(9, 'Data Scientist', 'Python'),
(10, None, 'Machine Learning'),
(11, None, 'Deep Learning'),
(12, None, 'Tableau')]


#Createing dataframe and view
df_job_skill=spark.createDataFrame(data,schema)
df_job_skill.createOrReplaceTempView("job_skill")
display(df_job_skill)



# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
w=Window.orderBy("row_id")
df_job_skill=df_job_skill.withColumn("flag",sum(when(col("job_role").isNotNull(),1).otherwise(0)).over(w))
w2=Window.partitionBy("flag").orderBy("row_id")
df_job_skill=df_job_skill.withColumn("job",first("job_role").over(w2)).select("row_id","job","skills")
display(df_job_skill)

# COMMAND ----------

# DBTITLE 1,SQL - SOLUTION
# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN job_role IS NOT NULL THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) OVER (
# MAGIC       ORDER BY
# MAGIC         row_id
# MAGIC     ) AS flag
# MAGIC   FROM
# MAGIC     job_skill
# MAGIC )
# MAGIC SELECT
# MAGIC   row_id,
# MAGIC   FIRST_VALUE(job_role) OVER (
# MAGIC     PARTITION BY flag
# MAGIC     ORDER BY
# MAGIC       row_id
# MAGIC   ) as job_role,
# MAGIC   skills
# MAGIC FROM
# MAGIC   CTE

# COMMAND ----------


