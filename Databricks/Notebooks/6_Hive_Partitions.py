# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists hive_practice
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/practice/hive_practice'

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database hive_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC use database hive_practice;

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/')

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/population.txt')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table population_dtls
# MAGIC (
# MAGIC id int, 
# MAGIC name string, 
# MAGIC salary int, 
# MAGIC gender string, 
# MAGIC deptno int, 
# MAGIC state string
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ','

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from population_dtls;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc formatted population_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC load data inpath '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/population.txt' into table population_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from population_dtls;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table population_dtls_static
# MAGIC (
# MAGIC id int, 
# MAGIC name string, 
# MAGIC salary int, 
# MAGIC gender string, 
# MAGIC deptno int
# MAGIC )
# MAGIC partitioned by (state string)
# MAGIC row format delimited
# MAGIC fields terminated by ','

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC desc formatted population_dtls_static

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show partitions population_dtls_static

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Andhrapradesh.txt
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Tamilanadu.txt

# COMMAND ----------

# MAGIC %sql 
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Andhrapradesh.txt'
# MAGIC into table population_dtls_static partition(state='TN')

# COMMAND ----------

# MAGIC %sql 
# MAGIC show partitions population_dtls_static;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from population_dtls_static;

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Andhrapradesh.txt')

# COMMAND ----------

# MAGIC %sql 
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Andhrapradesh.txt'
# MAGIC into table population_dtls_static partition(state = 'Andrapradesh')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from population_dtls_static;

# COMMAND ----------

# MAGIC %sql 
# MAGIC show partitions population_dtls_static

# COMMAND ----------

Dynamic Partition

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table population_dtls_dynamic
# MAGIC (
# MAGIC p_id int, 
# MAGIC p_name string, 
# MAGIC p_salary int, 
# MAGIC p_gender string, 
# MAGIC p_deptno int
# MAGIC )
# MAGIC partitioned by (p_state string)
# MAGIC row format delimited
# MAGIC fields terminated by ','

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from  population_dtls_dynamic;

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc population_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC set hive.exec.dynamic.partition.mode=nonstrict

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into population_dtls_dynamic partition(p_state)
# MAGIC select 
# MAGIC id as p_id,
# MAGIC name as p_name,
# MAGIC salary as p_salary, 
# MAGIC gender as p_gender, 
# MAGIC deptno as p_deptno,
# MAGIC state as p_state
# MAGIC from population_dtls;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from population_dtls_dynamic;

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions population_dtls_dynamic;

# COMMAND ----------


