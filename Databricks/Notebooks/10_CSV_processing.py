# Databricks notebook source
emp_dtls_new.csv
----------------
id,name,salary,gender,deptno
101,Prasad,25000,M,10
102,Rahul,30000,M,20
103,aditya,40000,M,30
104,Pavani,20000,F,40
105,anupama,35000,F,50
106,viswas,25000,M,10
107,akhila,45000,F,20
108,prajol,35000,M,30

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls_new.csv")

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls_new.csv'
df=spark.read.format("csv").load(filepath)
df.display()

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls_new.csv'
df=spark.read.format("csv").option("Header",True).load(filepath)
df.display()

# COMMAND ----------

By default, the datatypes were assigned to string data type.
We can convert into required format, by below methods.
1. Inferschema - It will scan the data rowwise and convert the data type to required format. But it is time cosumes and will decrease performance.
2. Schema define- Instead, we can define schema of the table along with column name and data type. We can call schema while loading data.

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_dtls_new.csv'
df=spark.read.format("csv").option("Header",True).option("inferschema",True).load(filepath)
df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

cust_schema=StructType(
    [
        StructField("id",IntegerType()),
        StructField("name",StringType()),
        StructField("salary",IntegerType()),
        StructField("gender",StringType()),
        StructField("deptno",IntegerType()),
    ]
)

# COMMAND ----------

df=spark.read.format("csv").option("Header",True).schema(cust_schema).load(filepath)
df.display()

# COMMAND ----------

prodid|prodname|price
P1|moble|10000
P2|laptop|20000
P3|desktop|5000
dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/prod.csv

# COMMAND ----------

prod_filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/prod.csv'
df1=spark.read.format("csv").option("Header",True).option("sep",'|').load(prod_filepath)
df1.display()

# COMMAND ----------

emp_badrecords.csv 
----------------
id,name,gender,salary
101,srinivvas,M,20000
102,Phani,M,30000
103,Lakshmi,F,FourtyThousand
104,Kiran,M,FiftyThousand
105,Mahesh,M,60000
106,Ravi,M,30000

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv

# COMMAND ----------

cust_schema = StructType(
    [
        StructField("id",IntegerType()),
        StructField("name",StringType()),
        StructField("gender",StringType()),
        StructField("salary",IntegerType())
        
    ]
)

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").schema(cust_schema).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv")

df1.display()

# COMMAND ----------

df1 = spark.read.format("csv")\
    .option("header", "true")\
    .schema(cust_schema)\
    .option("badrecordspath","dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/temp") \
    .load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv")

df1.display()

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/temp/20250319T165012/bad_records/part-00000-ec453334-66cc-4f83-bbb5-c0e877b66213")

# COMMAND ----------

modes

PERMISSIVE --> NULL (default)
DROPMALFORMED -->drop the bad records 
FAILFAST --> faile the dataframe

# COMMAND ----------

df1 = spark.read.format("csv")\
    .option("header", "true")\
    .schema(cust_schema)\
    .option("mode","PERMISSIVE") \
    .load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv")

df1.display()

# COMMAND ----------

df1 = spark.read.format("csv")\
    .option("header", "true")\
    .schema(cust_schema)\
    .option("mode","DROPMALFORMED") \
    .load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv")

df1.display()

# COMMAND ----------

df1 = spark.read.format("csv")\
    .option("header", "true")\
    .schema(cust_schema)\
    .option("mode","FAILFAST") \
    .load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp_badrecords.csv")

df1.display()
