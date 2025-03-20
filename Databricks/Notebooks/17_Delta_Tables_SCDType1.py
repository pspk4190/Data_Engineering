# Databricks notebook source
'''
 notebook : 17_Delta_tables_SCD_Type1
 Author   : Pavan 
 Date     : 20t March 2024
 description : implementing SCD Type1
'''

# COMMAND ----------

'''

 raw/
    cust_jan.csv
 
 bronze/
            cust_stg
 
 silver/
           customers
  
 archive/
 
 '''

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/raw')

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_jan.csv'
df=spark.read.format("csv").option("header",True).load(filepath)
df.display()

# COMMAND ----------

raw            bronze            silver           archive 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists Bronze
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/Bronze'

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze.cust_stg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.cust_stg

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists silver
# MAGIC location '/FileStore/shared_uploads/kpavankumar335@gmail.com/silver'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table if not exists silver.customers 
# MAGIC (
# MAGIC   cust_id string, 
# MAGIC   cust_name string, 
# MAGIC   address string
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC merge into silver.customers tgt 
# MAGIC using bronze.cust_stg stg 
# MAGIC on tgt.cust_id = stg.cust_id
# MAGIC when matched and tgt.address <> stg.address then update set * 
# MAGIC when not matched then insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers;

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/archive')

# COMMAND ----------

files_list=dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/raw')
for file in files_list:
    dbutils.fs.mv(file.path,'/FileStore/shared_uploads/kpavankumar335@gmail.com/archive')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/archive')

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_feb.csv

# COMMAND ----------

2nd month data

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_feb.csv'
df=spark.read.format("csv").option("header",True).load(filepath)
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze.cust_stg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC merge into silver.customers tgt 
# MAGIC using bronze.cust_stg stg 
# MAGIC on tgt.cust_id = stg.cust_id
# MAGIC when matched and tgt.address <> stg.address then update set * 
# MAGIC when not matched then insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers;
