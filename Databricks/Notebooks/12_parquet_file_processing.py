# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

filepath = 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_7.json'
df = spark.read.format("json").option("multiline",True).load(filepath)
df = df.select("id","gender","name","address.city", "address.country",explode("languages").alias("lang"))
df.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/empdata_p")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/empdata_p")

# COMMAND ----------

path='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/empdata_p/part-00000-tid-6168329158392376217-58f94f1e-b2cd-40bf-8d16-21f33a4a698b-46-1-c000.snappy.parquet'

# COMMAND ----------

path = 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/empdata_p/part-00000-tid-6168329158392376217-58f94f1e-b2cd-40bf-8d16-21f33a4a698b-46-1-c000.snappy.parquet'

df = spark.read.format("parquet").load(path)

df.display()
