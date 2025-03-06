# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %md
# MAGIC Creation of directory in DBFS:
# MAGIC syntax : dbutils.fs.mkdirs('location of file system')

# COMMAND ----------

dbutils.fs.mkdirs('shared_uploads/kpavankumar335@gmail.com/Training')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database hive_practice
# MAGIC location '/Filestore/shared_uploads/kpavankumar335@gmail.com/Training/hive_practice'

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use database hive_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_dtls
# MAGIC (
# MAGIC   id int,
# MAGIC   name varchar (20),
# MAGIC   location varchar (20)
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ','

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted emp_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/Filestore/shared_uploads/kpavankumar335@gmail.com/Training/hive_practice/emp_dtls')

# COMMAND ----------

dbutils.fs.ls('dbfs:/Filestore/shared_uploads/kpavankumar335@gmail.com/Training/hive_practice/emp_dtls')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/emp_dtls/emp.txt')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

# MAGIC %sql
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp.txt' into table emp_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table emp_dtls values (5,'Kumar','Hyderabad')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/Filestore/shared_uploads/kpavankumar335@gmail.com/Training/hive_practice/emp_dtls')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emp_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/Filestore/shared_uploads/kpavankumar335@gmail.com/Training/hive_practice/emp_dtls')

# COMMAND ----------

#creation of external table or unmanaged table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_dtls(
# MAGIC   id int,
# MAGIC   name varchar(20),
# MAGIC   location varchar(20)
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ','
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/'

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted emp_dtls

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_dtls values(5,'Sravan','Hyderbad');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table emp_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/')
