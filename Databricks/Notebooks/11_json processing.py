# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data")

# COMMAND ----------

emp_details_0.json
------------------

{"id":101,"name":"srinivas","gender":"male","language":"english","location":"hyderabad"}

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_0.json

# COMMAND ----------

filepath = 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_0.json'
df = spark.read.format("json").load(filepath)
df.display()

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_1.json
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_2.json
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_3.json
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_4.json
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_5.json

# COMMAND ----------

emp_details_1.json
------------------
{
"id":"1",
"name":"srinivas",
"gender":"male",
"language":"english",
"location":"hyderabad"
}

# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_1.json"
df=spark.read.format("json").option("multiline",True).load(filepath)
df.display()

# COMMAND ----------

emp_details_2.json
----------------
[
{
"id":"101",
"name":"srinivas",
"gender":"male"
},
{
"id":"102",
"name":"phani",
"gender":"male"
}
]

# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_2.json"
df=spark.read.format("json").option("multiline",True).load(filepath)
df.display()

# COMMAND ----------

df.createOrReplaceTempView("emp_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from emp_v;

# COMMAND ----------

emp_details_3.json
----------------
[
{
"id":"101",
"name":"srinivas",
"gender":"male",
"phonenumber":9999
},
{
"id":"102",
"name":"phani",
"gender":"male",
"location":"Hyderabad"
}
]

# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_3.json"
df=spark.read.format("json").option("multiline",True).load(filepath)
df.display()

# COMMAND ----------

emp_details_4.json
--------------------
[
{
"id":"101",
"name":"srinivas",
"gender":"male",
"address":{
            "city":"Hyderabad",
			"country":"india"
          } 
},
{
"id":"102",
"name":"phani",
"gender":"male",
"address":{
            "city":"Hyderabad",
			"country":"india"
          }
}
]

# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_4.json"
df=spark.read.format("json").option("multiline",True).load(filepath)
df.display()

# COMMAND ----------

df1=df.select("id","name","gender","address.city","address.country")
df1.display()

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp.csv")

# COMMAND ----------

#converting from csv to json
df=spark.read.format("csv").option("Header",True).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp.csv")
df.write.format("json").mode("overwrite").save("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/empdata_j")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/empdata_j")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table emp_dtls_j51
# MAGIC using json 
# MAGIC location 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/empdata_j'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls_j51;

# COMMAND ----------

df.write.format("json").mode("overwrite").saveAsTable("emp_dtls_jj_51")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls_jj_51;

# COMMAND ----------

df.write.format("json").mode("append").saveAsTable("emp_dtls_jjj_51")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dtls_jjj_51;

# COMMAND ----------

emp_details_5.json
  -------------------------
     [
{
"id":"101",
"name":"srinivas",
"gender":"male",
"address":{
            "city":"Hyderabad",
			"country":"india",
			"house_dtls":{
			                "doorno":"1-2-3",
							"street":"srnagar"
			             }
          } 
},
{
"id":"102",
"name":"phani",
"gender":"male",
"address":{
            "city":"Hyderabad",
			"country":"india",
			"house_dtls":{
			                "doorno":"1-2-3",
							"street":"ameerpet"
			             }
          }
}
]


# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_5.json"
df=spark.read.format("json").option("multiline",True).load(filepath)
df.display()

# COMMAND ----------

df2=df.select("id","name","gender","address.city","address.country","address.house_dtls.doorno","address.house_dtls.street")
df2.display()

# COMMAND ----------

df2.createOrReplaceTempView("Emp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from
# MAGIC Emp_view

# COMMAND ----------

emp_details_6.json
----------------
[
{
"id":"101",
"name":"srinivas",
"gender":"male",
"languages":["English","Hindi"]
},
{
"id":"102",
"name":"phani",
"gender":"male",
"languages":["English","Hindi"]
}
]

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_6.json

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

filepath = 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_6.json'
df = spark.read.format("json").option("multiline",True).load(filepath)
df1 = df.select("id","gender","name",explode("languages").alias("lang"))
df1.display()

# COMMAND ----------

emp_details_7.json
-----------------
[
{
"id":"101",
"name":"srinivas",
"gender":"male",
"languages":["English","Hindi"],
"address":{
            "city":"Hyderabad",
			"country":"india"
          } 
},
{
"id":"102",
"name":"phani",
"gender":"male",
"languages":["English","Hindi"],
"address":{
            "city":"Hyderabad",
			"country":"india"
          }
}
]

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_7.json

# COMMAND ----------

filepath = 'dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_details_7.json'
df = spark.read.format("json").option("multiline",True).load(filepath)
df = df.select("id","gender","name","address.city", "address.country",explode("languages").alias("lang"))
df.display()
