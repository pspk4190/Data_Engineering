# Databricks notebook source
'''
 notebook : 18_Delta_tables_SCD_Type2
 Author   : Pavan 
 Date     : 20th March
 description : implementing SCD Type2
'''

# COMMAND ----------

cust_may.csv
----------
cust_id,cust_name,address
C1,srinivas,Hyderabad
C2,Phani,Chennai
C3,Ram,Delhi

cust_june.csv
------------
cust_id,cust_name,address
C1,srinivas,bangalore
C2,Phani,Chennai
C3,Ram,Hyderabad
C4,Krishna,Hyderabad

cust_july.csv
------------
cust_id,cust_name,address
C1,srinivas,bangalore
C2,Phani,Pune
C3,Ram,Hyderabad
C4,Krishna,Hyderabad
C5,Mahesh,Bangalore


# COMMAND ----------

'''

 raw/
    cust_may.csv
 
 bronze/
            cust_stg
 
 silver/
           customers
  
 archive/

 '''

# COMMAND ----------

'/FileStore/shared_uploads/kpavankumar335@gmail.com/raw'
'/FileStore/shared_uploads/kpavankumar335@gmail.com/archive'

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_may.csv

# COMMAND ----------

df=spark.read.format("csv").option("header",True).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_may.csv")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze.cust_stg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.cust_stg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create table if not exists silver.customer_dtls
# MAGIC (
# MAGIC   cust_id string, 
# MAGIC   cust_name string, 
# MAGIC   address string,
# MAGIC   start_date date, 
# MAGIC   end_date date, 
# MAGIC   isActive string
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC merge into silver.customer_dtls tgt 
# MAGIC using
# MAGIC (
# MAGIC select
# MAGIC cs.cust_id as mergekey,
# MAGIC cs.*
# MAGIC from bronze.cust_stg cs 
# MAGIC
# MAGIC union all 
# MAGIC
# MAGIC select 
# MAGIC NULL as mergekey, 
# MAGIC cs.*
# MAGIC from bronze.cust_stg cs 
# MAGIC join silver.customer_dtls tgt 
# MAGIC on cs.cust_id= tgt.cust_id 
# MAGIC and tgt.address <> cs.address
# MAGIC and tgt.isActive = 'Y'
# MAGIC )src
# MAGIC on src.mergekey = tgt.cust_id 
# MAGIC when matched and src.address <> tgt.address then update set end_date = current_date()-1 , isActive = 'N'
# MAGIC when not matched then 
# MAGIC insert 
# MAGIC (
# MAGIC tgt.cust_id, 
# MAGIC tgt.cust_name, 
# MAGIC tgt.address, 
# MAGIC tgt.start_date, 
# MAGIC tgt.end_date, 
# MAGIC tgt.isActive
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC src.cust_id ,
# MAGIC src.cust_name, 
# MAGIC src.address, 
# MAGIC current_Date(), 
# MAGIC null,
# MAGIC 'Y' 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customer_dtls;

# COMMAND ----------

files_list=dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/raw')
for file in files_list:
    dbutils.fs.mv(file.path,'/FileStore/shared_uploads/kpavankumar335@gmail.com/archive')

# COMMAND ----------

Next month data


# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_june.csv

# COMMAND ----------

df=spark.read.format("csv").option("header",True).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/raw/cust_june.csv")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze.cust_stg")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC merge into silver.customer_dtls tgt 
# MAGIC using
# MAGIC (
# MAGIC select
# MAGIC cs.cust_id as mergekey,
# MAGIC cs.*
# MAGIC from bronze.cust_stg cs 
# MAGIC
# MAGIC union all 
# MAGIC
# MAGIC select 
# MAGIC NULL as mergekey, 
# MAGIC cs.*
# MAGIC from bronze.cust_stg cs 
# MAGIC join silver.customer_dtls tgt 
# MAGIC on cs.cust_id= tgt.cust_id 
# MAGIC and tgt.address <> cs.address
# MAGIC and tgt.isActive = 'Y'
# MAGIC )src
# MAGIC on src.mergekey = tgt.cust_id 
# MAGIC when matched and src.address <> tgt.address then update set end_date = current_date()-1 , isActive = 'N'
# MAGIC when not matched then 
# MAGIC insert 
# MAGIC (
# MAGIC tgt.cust_id, 
# MAGIC tgt.cust_name, 
# MAGIC tgt.address, 
# MAGIC tgt.start_date, 
# MAGIC tgt.end_date, 
# MAGIC tgt.isActive
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC src.cust_id ,
# MAGIC src.cust_name, 
# MAGIC src.address, 
# MAGIC current_Date(), 
# MAGIC null,
# MAGIC 'Y' 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customer_dtls;

# COMMAND ----------

files_list=dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/raw')
for file in files_list:
    dbutils.fs.mv(file.path,'/FileStore/shared_uploads/kpavankumar335@gmail.com/archive')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/raw')
