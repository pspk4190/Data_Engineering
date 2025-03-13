# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/review_17122024.csv
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/review_16122024.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists stage
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/stage'

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/stage')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table if not exists stage.reviews_stg
# MAGIC (
# MAGIC   review_id string, 
# MAGIC   review_txt string, 
# MAGIC   review_dt string, 
# MAGIC   prod_id string
# MAGIC )
# MAGIC row format delimited
# MAGIC fields terminated by ',';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/review_16122024.csv' into table stage.reviews_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stage.reviews_stg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create database if not exists target
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound//target'

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists target.reviews 
# MAGIC (
# MAGIC   review_id string, 
# MAGIC   review_txt string, 
# MAGIC   prod_id string
# MAGIC )
# MAGIC partitioned by (review_dt string)
# MAGIC row format delimited
# MAGIC fields terminated by ','
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into target.reviews partition(review_dt)
# MAGIC select 
# MAGIC review_id, 
# MAGIC review_txt,
# MAGIC prod_id,
# MAGIC review_dt 
# MAGIC from stage.reviews_stg

# COMMAND ----------

# MAGIC %sql 
# MAGIC set  hive.exec.dynamic.partition.mode=nonstrict

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into target.reviews partition(review_dt)
# MAGIC select 
# MAGIC review_id, 
# MAGIC review_txt,
# MAGIC prod_id,
# MAGIC review_dt 
# MAGIC from stage.reviews_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target.reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from stage.reviews_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC load data inpath 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/review_17122024.csv' overwrite into table stage.reviews_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from stage.reviews_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into target.reviews partition(review_dt)
# MAGIC select 
# MAGIC review_id, 
# MAGIC review_txt,
# MAGIC prod_id,
# MAGIC review_dt 
# MAGIC from stage.reviews_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target.reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions target.reviews;
