# Databricks notebook source
emp_dtls_new.csv
-----------------
id,name,salary,gender,deptno
101,Prasad,25000,M,10
102,Rahul,30000,M,20
103,aditya,40000,M,30
104,Pavani,20000,F,40
105,anupama,35000,F,50
106,viswas,25000,M,10
107,akhila,45000,F,20
108,prajol,35000,M,30
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls.csv

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls.csv")
df.display()

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------

def genderupdate(str):
    if str=='M':
        return 'Male'
    else:
        return 'Female'
gen_udf=udf(genderupdate)

# COMMAND ----------

df = df.withColumn("gender",gen_udf("gender"))

df.display()

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls.csv")
df.createOrReplaceTempView("emp_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_v

# COMMAND ----------

def genderupdate(str):
    if str == 'M':
        return 'male'
    else:
        return 'female'
    
spark.udf.register("gen_udf",genderupdate)

gen_udf = udf(genderupdate)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC id,
# MAGIC name, 
# MAGIC salary, 
# MAGIC gen_udf(gender) as gender,
# MAGIC deptno
# MAGIC from emp_v

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

combobox(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a combobox input widget with a given name, default value and choices

dropdown(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a dropdown input widget a with given name, default value and choices

get(name: String): String -> Retrieves current value of an input widget

multiselect(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a multiselect input widget with a given name, default value and choices

remove(name: String): void -> Removes an input widget from the notebook

removeAll: void -> Removes all widgets in the notebook

text(name: String, defaultValue: String, label: String): void -> Creates a text input widget with a given name and default value

# COMMAND ----------

fruits_lst = ['apple','banana','cherry','mango','papaya']

# COMMAND ----------

#dropdown(name: String, defaultValue: String, choices: Seq, label: String)
dbutils.widgets.dropdown('w_dropdown','mango',fruits_lst,'fruitName')

fruitname = dbutils.widgets.get('w_dropdown')

print("fruit name is ",fruitname)

# COMMAND ----------

dbutils.widgets.multiselect('w_multiselect','mango',fruits_lst,'fruitName')

fruitname = dbutils.widgets.get('w_multiselect')

print("fruit name is ",fruitname)

# COMMAND ----------

dbutils.widgets.combobox('w_combobox','mango',fruits_lst,'fruitName')

fruitname = dbutils.widgets.get('w_combobox')

print("fruit name is ",fruitname)

# COMMAND ----------

dbutils.widgets.text('w_txt','mango','fruitName')

fruitname = dbutils.widgets.get('w_txt')

print("fruit name is ",fruitname)

# COMMAND ----------

dbutils.widgets.remove('w_txt')

# COMMAND ----------

dbutils.widgets.removeAll()
