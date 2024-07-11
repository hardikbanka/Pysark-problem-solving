# Databricks notebook source
# MAGIC %md
# MAGIC # SQL -Question 5

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
#Using the given Salary, Income and Deduction tables, first write an sql query to populate the Emp_Transaction table as shown below and then generate a salary report as shown.

			
	SALARY		
	EMP_ID	EMP_NAME	BASE_SALARY
	1	Rohan	5000
	2	Alex	6000
	3	Maryam	7000
			
			
	INCOME		
	ID	INCOME	PERCENTAGE
	1	Basic	100
	2	Allowance	4
	3	Others	6
			
			
	DEDUCTION		
	ID	DEDUCTION	PERCENTAGE
	1	Insurance	5
	2	Health	6
	3	House	4
			


					
	EXPECTED OUTPUT - EMP_TRANSACTION				
	EMP_ID	EMP_NAME	TRNS_TYPE	AMOUNT	
	1	Rohan	Insurance	250	
	2	Alex	Insurance	300	
	3	Maryam	Insurance	350	
	1	Rohan	House	200	
	2	Alex	House	240	
	3	Maryam	House	280	
	1	Rohan	Basic	5000	
	2	Alex	Basic	6000	
	3	Maryam	Basic	7000	
	1	Rohan	Health	300	
	2	Alex	Health	360	
	3	Maryam	Health	420	
	1	Rohan	Allowance	200	
	2	Alex	Allowance	240	
	3	Maryam	Allowance	280	
	1	Rohan	Others	300	
	2	Alex	Others	360	
	3	Maryam	Others	420	


									
EXPECTED OUTPUT - SALARY REPORT									
EMPLOYEE	BASIC	ALLOWANCE	OTHERS	GROSS	INSURANCE	HEALTH	HOUSE	TOTAL_DEDUCTIONS	NET_PAY
Alex	6000	240	360	6600	300	360	240	900	5700
Maryam	7000	280	420	7700	350	420	280	1050	6650
Rohan	5000	200	300	5500	250	300	200	750	4750




# COMMAND ----------

# DBTITLE 1,PYSPARK - SOLUTION
# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,min,max,lit,when,sum,explode
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

# Spark Session Initialize
spark=SparkSession.builder.appName("Salary").getOrCreate()

# Schemas for all the 3 Dataframes
schema_salary=StructType([StructField("emp_id",IntegerType(),False),
                          StructField("emp_name",StringType(),True),
                          StructField("base_salary",IntegerType(),True)])

schema_income=StructType([StructField("id",IntegerType(),False),
                          StructField("income",StringType(),True),
                          StructField("percentage",IntegerType(),True)])


schema_deduc=StructType([StructField("id",IntegerType(),False),
                          StructField("deduction",StringType(),True),
                          StructField("percentage",IntegerType(),True)])

# Data for all the 3 dataframes  
data_salary=[(1, 'Rohan', 5000),(2, 'Alex', 6000),(3, 'Maryam', 7000)]
data_income=[(1,'Basic', 100),(2,'Allowance', 4),(3,'Others', 6)]
data_deduc=[(1,'Insurance', 5),(2,'Health', 6),(3,'House', 4)]

# DataFrames is created 
salary=spark.createDataFrame(data_salary,schema_salary)
income=spark.createDataFrame(data_income,schema_income)
deduc=spark.createDataFrame(data_deduc,schema_deduc)

# Temporary view creation for 3 dataframes
salary.createOrReplaceTempView("salary")
income.createOrReplaceTempView("income")
deduc.createOrReplaceTempView("deduc")



# COMMAND ----------

# DBTITLE 1,PYSPARK OUTPUT -1
# Cross join the dataframes
df_1=(salary.crossJoin(income)
                  .withColumnRenamed("income","tran_type")
                  .withColumn("amount",(col("base_salary")*(col("percentage")/100)))
                  .select("emp_id","emp_name","tran_type","amount"))

# Cross join the dataframes
df_2=(salary.crossJoin(deduc)
                  .withColumnRenamed("deduction","trans_type")
                  .withColumn("amount",(col("base_salary")*(col("percentage")/100)))
                  .select("emp_id","emp_name","trans_type","amount"))
df_final=df_1.unionAll(df_2)

# COMMAND ----------

# DBTITLE 1,Pyspark - Output 2
# Pivot the table to convert it into desired result
df_pivot=df_final.groupBy("emp_name").pivot("tran_type").agg(sum("amount"))
df_pivot=df_pivot.withColumn("Total_deductions",(col("Insurance")+col("Health")+col("House"))).withColumn("gross",col("Basic")+col("allowance")+col("others")).withColumn("Net_pay",col("gross")-col("Total_deductions"))


# COMMAND ----------

# DBTITLE 1,SQL - OUTPUT 1
# MAGIC %sql
# MAGIC Select emp_id,emp_name,income as trns_type,(base_salary*(i.percentage/100)) as amount from salary 
# MAGIC cross join income i
# MAGIC union all
# MAGIC Select emp_id,emp_name,deduction as trns_type,(base_salary*(d.percentage/100)) as amount from salary 
# MAGIC cross join deduc d
# MAGIC

# COMMAND ----------

# DBTITLE 1,SQL - OUTPUT 2
# MAGIC %sql
# MAGIC with emp_transaction as (Select emp_id,emp_name,income as trns_type,(base_salary*(i.percentage/100)) as amount from salary 
# MAGIC cross join income i
# MAGIC union all
# MAGIC Select emp_id,emp_name,deduction as trns_type,(base_salary*(d.percentage/100)) as amount from salary 
# MAGIC cross join deduc d)
# MAGIC
# MAGIC SELECT emp_name,
# MAGIC   SUM(CASE WHEN trns_type = 'basic' THEN amount END) AS BASIC,
# MAGIC   SUM(CASE WHEN trns_type = 'ALLOWANCE' THEN amount ELSE 0 END) AS ALLOWANCE,
# MAGIC   SUM(CASE WHEN trns_type = 'OTHERS' THEN amount ELSE 0 END) AS OTHERS,
# MAGIC   SUM(CASE WHEN trns_type = 'GROSS' THEN amount ELSE 0 END) AS GROSS,
# MAGIC   SUM(CASE WHEN trns_type = 'INSURANCE' THEN amount ELSE 0 END) AS INSURANCE,
# MAGIC   SUM(CASE WHEN trns_type = 'HEALTH' THEN amount ELSE 0 END) AS HEALTH,
# MAGIC   SUM(CASE WHEN trns_type = 'HOUSE' THEN amount ELSE 0 END) AS HOUSE,
# MAGIC   SUM(CASE WHEN trns_type = 'TOTAL_DEDUCTIONS' THEN amount ELSE 0 END) AS TOTAL_DEDUCTIONS,
# MAGIC   SUM(CASE WHEN trns_type = 'NET_PAY' THEN amount ELSE 0 END) AS NET_PAY
# MAGIC FROM emp_transaction
# MAGIC GROUP BY emp_name
