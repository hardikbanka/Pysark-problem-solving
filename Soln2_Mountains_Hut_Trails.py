# Databricks notebook source
# MAGIC %md
# MAGIC #SQL - Qusetion 2

# COMMAND ----------

# DBTITLE 1,PROBLEM - STATEMENT
# A ski resort company is planning to construct a new ski slope using a pre-existing network of 
# mountain huts and trails between them. A new slope has to begin at one of the mountain huts, 
# have a middle station at another hut connected with the first one by a direct trail, and end at 
# the third mountain hut which is also connected by a direct trail to the second hut. The altitude 
# of the three huts chosen for constructing the ski slope has to be strictly decreasing.
# You are given two SQL tables, mountain_huts and trails, with the following structure:
# create table mountain_huts (
#  id integer not null,
#  name varchar(40) not null,
#  altitude integer not null,
#  unique(name),
#  unique(id)
#  );
# create table trails (
#  hut1 integer not null,
#  hut2 integer not null
#  );
# insert into mountain_huts values (1, 'Dakonat', 1900);
# insert into mountain_huts values (2, 'Natisa', 2100);
# insert into mountain_huts values (3, 'Gajantut', 1600);
# insert into mountain_huts values (4, 'Rifat', 782);
# insert into mountain_huts values (5, 'Tupur', 1370);
# insert into trails values (1, 3);
# insert into trails values (3, 2);
# insert into trails values (3, 5);
# insert into trails values (4, 5);
# insert into trails values (1, 5);
# Each entry in the table trails represents a direct connection between huts with IDs hut1 and 
# hut2. Note that all trails are bidirectional.
# Create a query that finds all triplets(startpt,middlept,endpt) representing the mountain huts 
# that may be used for construction of a ski slope.
# Output returned by the query can be ordered in any way.
# Examples:
# 1.Given the tables:
# mountain_huts:
# Id Name Altitude
# 1 Dakonat 1900
# 2 Natisa 2100
# 3 Gajantut 1600
# 4 Rifat 782
# 5 Tupur 1370
# trails:
# Hut1 Hut2
# 1 3
# 3 2
# 3 5
# 4 5
# 1 5
# Your query should return:
# startpt middlept endpt
# Dakonat Gajantut Tupur
# Dakonat Tupur Rifat
# Gajantut Tupur Rifat
# Natisa Gajantut Tupur
# Assume that:
#  there is no trail going from a hut back to itself;
#  for every two huts there is at most one direct trail connecting them;
#  each hut from table trails occurs in table mountain_huts;

# COMMAND ----------

# DBTITLE 1,SPARK- SOLUTION
# Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.types import StructField,IntegerType,StringType,StructType

# schema creation for Dataframes
schema_m=StructType([StructField("id",IntegerType()),
                   StructField("name",StringType()),
                   StructField("altitude",IntegerType())])

schema_h=StructType([StructField("hut1",IntegerType()),
                     StructField("hut2",IntegerType())])


# data for the two Dataframes
data_m=[(1, 'Dakonat', 1900),(2, 'Natisa', 2100),(3, 'Gajantut', 1600),(4, 'Rifat', 782),(5, 'Tupur', 1370)]

data_h=[(1, 3),(3, 2),(3, 5),(4, 5),(1, 5)]


# Create Dataframes and temporary views
df_m=spark.createDataFrame(data=data_m,schema=schema_m)
df_m.createOrReplaceTempView("mountain_huts")
df_h=spark.createDataFrame(data=data_h,schema=schema_h)
df_h.createOrReplaceTempView("trails")



# COMMAND ----------

# DBTITLE 1,SPARK- SOLUTION
# Joining two dataframes to get hut names and altitude details
df_ht=df_m.join(df_h,df_m.id==df_h.hut1)

# Renaming Columns to appropriate names
new_col={"name":"start_hut_name","altitude":"start_altitude","hut1":"start_hut","hut2":"end_hut"}
for i,j in new_col.items():
  df_ht=df_ht.withColumnRenamed(i,j)

# Drop id column as it is not required 
df_ht=df_ht.drop("id")

# joining the newly created dataframe again with mountains dataframe
df_ht2=df_ht.join(df_m,df_m.id==df_ht.end_hut)

# Renaming Columns to appropriate names
new_col2={"name":"end_hut_name","altitude":"end_altitude"}
for i,j in new_col2.items():
  df_ht2=df_ht2.withColumnRenamed(i,j)

# Drop id column as it is not required 
df_ht2=df_ht2.drop("id")

# Create a flag column to indicate start hut has lower altitude then end hut
df_ht2=df_ht2.withColumn("flag",when(col("start_altitude")>col("end_altitude"),1).otherwise(0))

# Interchanging the name of the hut if start hut has lower altitutde
df_ht3=df_ht2.withColumn(
    "start_hut1", when(col("flag") == 1, col("start_hut")).otherwise(col("end_hut"))
).withColumn(
    "start_hut_name1", when(col("flag") == 1, col("start_hut_name")).otherwise(col("end_hut_name")))

# Interchanging the name of the hut if start hut has lower altitutde
df_ht3=df_ht3.withColumn(
    "end_hut1", when(col("flag") == 1, col("end_hut")).otherwise(col("start_hut"))
).withColumn(
    "end_hut_name1", when(col("flag") == 1, col("end_hut_name")).otherwise(col("start_hut_name"))
).orderBy("start_hut")

# Drop the unwanted columns
df_ht3=df_ht3.drop("start_hut","start_hut_name","end_hut","end_hut_name","flag","start_altitude","end_altitude")

# Self join to get start point , middle point and end point
df_f1 = df_ht3.alias("df1")
df_f2 = df_ht3.alias("df2")

new_col_3={"start_hut1":"start_hut2","end_hut1":"end_hut2","start_hut_name1":"start_hut_name2","end_hut_name1":"end_hut_name2"}
for i,j in new_col_3.items():
  df_f2=df_f2.withColumnRenamed(i,j)


df_final = df_f1.join(
    df_f2,
    df_f1["end_hut1"] == df_f2["start_hut2"],
    "inner"
).select(
    df_f1["start_hut_name1"].alias("start_point"),
    df_f1["end_hut_name1"].alias("middle_point"),
    df_f2["end_hut_name2"].alias("end_point")
)

display(df_final)

# COMMAND ----------

# DBTITLE 1,SQL- SOLUTION
# MAGIC %sql
# MAGIC with cte_ht as (SELECT t.hut1 as start_hut,m.name as start_hut_name,m.altitude as start_altitude,t.hut2 as end_hut FROM mountain_huts m
# MAGIC JOIN trails t ON m.id=t.hut1),
# MAGIC
# MAGIC cte_ht2 (select  h1.*,m2.name as end_hut_name,m2.altitude as end_altitude, case when m2.altitude <start_altitude then 1 else 0 end as flag from cte_ht h1
# MAGIC join mountain_huts m2 on m2.id=h1.end_hut),
# MAGIC
# MAGIC
# MAGIC
# MAGIC cte_ht3(select case when flag=1 then start_hut else end_hut end as start_hut,
# MAGIC        case when flag=1 then start_hut_name else end_hut_name end as start_hut_name,
# MAGIC        case when flag=1 then end_hut else start_hut end as end_hut,
# MAGIC        case when flag=1 then end_hut_name else start_hut_name end as end_hut_name
# MAGIC from cte_ht2 order by start_hut)
# MAGIC
# MAGIC
# MAGIC
# MAGIC select c1.start_hut_name as startpy,c1.end_hut_name as middlepy,c2.end_hut_name as endpy  from cte_ht3 c1 join cte_ht3 c2
# MAGIC on c1.end_hut=c2.start_hut
# MAGIC

# COMMAND ----------


