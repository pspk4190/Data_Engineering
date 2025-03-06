# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/products')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/products')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table prod_dtls
# MAGIC (
# MAGIC   prod_id string, 
# MAGIC   prod_name string
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ','
# MAGIC location 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/products'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from prod_dtls

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/products')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder/prod_3.csv' into table prod_dtls

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prod_dtls;

# COMMAND ----------

Types of Tables
1. Internal tables or Managed tables
2. External tables or Unmanaged tables

Loading data into tables:
1. table pointing to the folder
2. load data inpath 
3. insert statements 

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc formatted prod_dtls;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into prod_dtls values('P7','desktop');

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/products')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC describe database hive_practice;

# COMMAND ----------

# MAGIC %sql 
# MAGIC use hive_practice;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC describe database hive_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE hive_practice;

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database hive_practice
# MAGIC location 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE hive_practice;

# COMMAND ----------

# MAGIC %sql 
# MAGIC use hive_practice;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table emp_dtls
# MAGIC (
# MAGIC   empid int, 
# MAGIC   empname string
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ',';

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into emp_dtls values(1,'krishna')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/emp_dtls')

# COMMAND ----------


