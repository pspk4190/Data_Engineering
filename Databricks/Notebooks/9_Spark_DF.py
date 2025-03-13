# Databricks notebook source
"""
Spark Dataframe is a 2 dimensional API, where data is represented ina named columns like a rows and columns in a relational database.
Creation of Dataframe:
1. SQLContext
2. SparkSession
3. CreateDataFrame
4. Spark.SQL
5. Spark.Read
6. Apply transformation on Dataframe
7. RDD to dataframe
8. Pandas dataframe to pyspark dataframe

"""

# COMMAND ----------

#SQLContext method: It is a deprecated method.

from pyspark.sql import SQLContext
from pyspark.sql import Row
sql_C= SQLContext (sc)

# COMMAND ----------

#Create Dataframe method

lst=[
    Row(id=101, name='Pavan'),
    Row(id=102, name='Naresh'),
    Row(id=103, name='Srinivas')
]
df=sql_C.createDataFrame(lst)
df.display()

# COMMAND ----------

#Creation of Spark Session

from pyspark.sql import SparkSession
SS=SparkSession.builder.getOrCreate()

# COMMAND ----------

lst=[
    (1,'Pavan','Male'),
    (2,'Srinivas','Male')
]

df=spark.createDataFrame(lst)
df.display()

# COMMAND ----------

lst=[
    (1,'Pavan','Male'),
    (2,'Srinivas','Male')
]

df=spark.createDataFrame(lst,schema=('id','name','gender'))
df.display()

# COMMAND ----------

import pyspark
dir(pyspark.sql.dataframe.DataFrame)

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp1.csv

Id,Name,Gender,Salary
101,Pavan,M,20000
102,Srinivas,M,30000
103,Lakshmi,F,20000
104,Jay,M,15000
105,Sravani,F,20000

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp1.csv'
df1=spark.read.format("csv").option("header",True).load(filepath)
df1.display()

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp1.csv'
df1=spark.read.format("csv").option("header",True).load(filepath)
df1.createOrReplaceTempView('emp_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC Gender, sum(Salary)
# MAGIC from emp_v
# MAGIC group by  Gender

# COMMAND ----------

from pyspark.sql.functions import lit
df2=df1.withColumn("Country", lit("India"))
df2.display()

# COMMAND ----------

#Converting RDD into DF

lst1=[(1,'Azure'),(2,'AWS'),(3,'GCP')]
rdd1=sc.parallelize(lst1)
rdd2=rdd1.map(lambda x:Row(id=x[0],name=x[1]))
rdd2.collect()


# COMMAND ----------

df=rdd2.toDF(["id","comp"])
df.display()

# COMMAND ----------

type(df)

# COMMAND ----------

#DF To RDD

res_rdd=df.rdd
res_rdd.collect()

# COMMAND ----------

#Pandas dataframe to pyspark dataframe

import pandas as pd
data=[(1,'aa'),(2,'bb'),(3,'cc'),(4,'dd')]
p_df=pd.DataFrame(data,columns=['id','name'])
p_df

# COMMAND ----------

s_df=spark.createDataFrame(p_df)
s_df.display()

# COMMAND ----------

#DF To Pandas DF
p_df2=s_df.toPandas()
type(p_df2)
