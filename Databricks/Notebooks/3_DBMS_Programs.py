# Databricks notebook source
dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder')
dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound_folder')

# COMMAND ----------

# MAGIC %md
# MAGIC       inbound/                   outbound/
# MAGIC            orders.txt
# MAGIC 		   products.txt 
# MAGIC 		   customers.txt

# COMMAND ----------

inbound_folder='/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder'
outbound_folder='/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound_folder'

# COMMAND ----------

dbutils.fs.ls(inbound_folder)

# COMMAND ----------

dbutils.fs.ls(inbound_folder)

# COMMAND ----------

lst = [10,20,30]

for i in lst:
    print(i+5)

# COMMAND ----------

files_lst = dbutils.fs.ls(inbound_folder)

# COMMAND ----------

files_lst

# COMMAND ----------

for file in files_lst:
    print(file.name)

# COMMAND ----------

for file in files_lst:
    print(file.path,'****', file.name)

# COMMAND ----------

dbutils.fs.ls(outbound_folder)

# COMMAND ----------

for file in files_lst:
    dbutils.fs.cp(file.path,outbound_folder)

# COMMAND ----------

dbutils.fs.ls(outbound_folder)

# COMMAND ----------

copy only csv files 

  inbound/                   outbound/
           orders.txt                  sales.csv
		   sales.csv                   products.csv
		   products.csv 
		   customers.txt

# COMMAND ----------

dbutils.fs.ls(inbound_folder)

# COMMAND ----------

files_lst = dbutils.fs.ls(inbound_folder)

# COMMAND ----------

for file in files_lst:
    print(file.name ,'***',file.path)

# COMMAND ----------

for file in files_lst:
    if file.name.endswith('csv'):
        print(file.name)

# COMMAND ----------

for file in files_lst:
    if file.name.endswith('csv'):
        dbutils.fs.ls(file.path)

# COMMAND ----------

dbutils.fs.ls(outbound_folder)

# COMMAND ----------

for file in files_lst:
    if file.name.endswith('csv'):
        dbutils.fs.cp(file.path,outbound_folder)

# COMMAND ----------

dbutils.fs.ls(outbound_folder)

# COMMAND ----------

  inbound/                          outbound/
          orders_02032025.txt                orders_03032025.txt               
          products_14122024.txt              products_03032025.txt
		  products_02032025.txt 
		  products_03032025.txt 

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder', True)

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder')

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder')

# COMMAND ----------

inbound_folder='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound_folder'

# COMMAND ----------

dbutils.fs.ls(inbound_folder)

# COMMAND ----------

from datetime import datetime 

dt = datetime.now()

filedt = dt.strftime("%d%m%Y")
print(filedt)

# COMMAND ----------

filename = 'orders_03032025.txt'
if filedt in filename:
    print("exists")
else:
    print("not exists")

# COMMAND ----------

file_list=dbutils.fs.ls(inbound_folder)

# COMMAND ----------

for file in file_list:
    print(file.name)

# COMMAND ----------

from datetime import datetime 
dt = datetime.now()
filedt = dt.strftime("%d%m%Y")
#print(filedt)

files_lst=dbutils.fs.ls(inbound_folder)

for file in files_lst:
    if filedt in file.name:
        print(file.name,'***', file.path)



# COMMAND ----------

dbutils.fs.ls(outbound_folder)

# COMMAND ----------

from datetime import datetime 

dt = datetime.now()

filedt = dt.strftime("%d%m%Y")
files_lst = dbutils.fs.ls(inbound_folder)

for file in files_lst:
    if filedt in file.name:
        dbutils.fs.cp(file.path,outbound_folder)

# COMMAND ----------

dbutils.fs.ls(outbound_folder)
