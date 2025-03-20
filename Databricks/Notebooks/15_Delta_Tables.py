# Databricks notebook source
# MAGIC %sql 
# MAGIC create database hivedemo
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo'

# COMMAND ----------

# MAGIC %sql 
# MAGIC use hivedemo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table accts_c
# MAGIC (
# MAGIC acct_id int, 
# MAGIC acct_name string  
# MAGIC )
# MAGIC using csv;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts_c values(1, 'srinivas')

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc formatted accts_c;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts_c')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table accts_j
# MAGIC (
# MAGIC acct_id int, 
# MAGIC acct_name string  
# MAGIC )
# MAGIC using json;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts_j values(1, 'phani');

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted accts_j;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts_j')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table accts_p
# MAGIC (
# MAGIC acct_id int, 
# MAGIC acct_name string  
# MAGIC )
# MAGIC using parquet;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc formatted accts_p

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts_p values(3,'krishna')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts_p')

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts_p/part-00000-tid-6508653119818901660-401b5f6b-612b-477e-adb3-5550d944c4f8-35-1-c000.snappy.parquet

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

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts/_delta_log/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into accts values(1, 'srinivas');

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/hivedemo/accts/_delta_log/')

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

# MAGIC %sql
# MAGIC select * from accts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC restore table accts version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from accts;

# COMMAND ----------


