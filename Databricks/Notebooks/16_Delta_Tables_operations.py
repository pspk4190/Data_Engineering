# Databricks notebook source
# MAGIC %sql 
# MAGIC create database hivepractice
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice'

# COMMAND ----------

# MAGIC %sql 
# MAGIC use hivepractice;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table accts
# MAGIC (
# MAGIC acct_id int, 
# MAGIC acct_name string  
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC desc formatted accts;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts/_delta_log/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts values(1, 'srinivas');

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts/_delta_log/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts values(2, 'phani');

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts values(3,'krishna')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC update accts set acct_name= 'krish' where acct_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts/_delta_log/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')


# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from accts where acct_id = 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from accts

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC optimize accts

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df = spark.read.format("parquet").load('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts/part-00000-51f8b5f6-e28b-441f-ac65-f29d5e1f2eba-c000.snappy.parquet')

df.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC vacuum  accts retain 0 hours dry run

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC vacuum  accts retain 0 hours

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivepractice/accts')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from accts version as of 3

# COMMAND ----------

# MAGIC %sql 
# MAGIC optimize accts zorder by (acct_id)
