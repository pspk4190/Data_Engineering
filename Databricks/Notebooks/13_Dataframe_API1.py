# Databricks notebook source
emp_information.csv
-------------------
id,firstname,lastname,age,salary,gender,deptno,state,mobilenumber,mail
101,Ravi,jain,23,2000,M,10,Tamilnadu,9820102292,Ravi@gmail.com
102,Krish,agarwal,25,3000,M,10,Karnataka,9820102293,Krish@gmail.com
103,Rama,Reddy,38,4000,F,20,Tamilnadu,9820102294,Rama@gmail.com
104,Ramu,kochar,43,5000,M,20,Andrapradesh,9820102295,Ramu@gmail.com
105,lalitha,ambani,51,4000,F,10,Kerala,9820102296,lalitha@gmail.com
106,Suresh,modi,50,4000,M,30,Andrapradesh,9820102297,Suresh@gmail.com
107,Mahesh,agarwal,34,3000,M,40,Tamilnadu,9820102298,Mahesh@gmail.com
108,Lakshmi,ambani,30,8000,F,30,Andrapradesh,9820102299,Lakshmi@gmail.com
109,Satish,Reddy,52,7000,M,50,Kerala,9820102300,Satish@gmail.com
110,Anusha,modi,70,3000,F,40,Karnataka,9820102301,Anusha@gmail.com

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data")

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_information.csv

# COMMAND ----------

filepath="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_information.csv"
df=spark.read.format("csv").option("Header",True).load(filepath)
df.display()

# COMMAND ----------

df.columns

# COMMAND ----------

import pyspark 

dir(pyspark.sql.dataframe.DataFrame)

# COMMAND ----------

df1 = df.select("id","firstname","salary","gender","deptno")
df1.display()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#df1 = df.select("id","firstname","salary","gender","deptno")
df1 = df.select(col("id"),col("firstname"),col("salary"),col("gender"),col("deptno"))
#df1 = df.select(df.id , df.firstname, df.salary , df.gender, df.deptno)
df1.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("emp_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC id,
# MAGIC firstname,
# MAGIC concat(firstname,' ', lastname) as fullname,
# MAGIC gender,
# MAGIC salary,
# MAGIC deptno
# MAGIC from
# MAGIC emp_v

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn("fullname",concat(df.firstname ,df.lastname))
df.display()

# COMMAND ----------

df = df.withColumn("salary",df.salary.cast('int'))
df.display()

# COMMAND ----------

df = df.withColumn("salary",df.salary + 10000)

df.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC id,
# MAGIC firstname,
# MAGIC concat(firstname, lastname) as fullname,
# MAGIC case when gender = 'M' then 'male' else 'female' end as gender,
# MAGIC salary+10000 as salary,
# MAGIC deptno
# MAGIC from emp_v

# COMMAND ----------

df = df.withColumn("gender", when(df.gender == 'M','male').otherwise('female'))

df.display()

# COMMAND ----------

df = df.drop('mail')

df.display()

# COMMAND ----------

cols_lst = ['state','mobilenumber']

df = df.drop(*cols_lst)

df.display()

# COMMAND ----------

df = df.withColumnRenamed("id","empid")

df.display()

# COMMAND ----------

df1 = df.filter(df.deptno == 30)
df1.display()

# COMMAND ----------

df1 = df.filter((df.deptno == 30) & (df.gender=='female'))
df1.display()

# COMMAND ----------

df1 = df.groupBy("deptno").agg(sum("salary").alias("totalsalary"))

df1.display()

# COMMAND ----------

df1 = df.groupBy("deptno").agg(sum("salary").alias("totalsalary"))

df1 = df1.filter(df1.totalsalary < 20000)

df1.display()

# COMMAND ----------

df1 = df.groupBy("deptno").agg(  sum("salary").alias("totalsalary"),\
                                  min("salary").alias("minimumsalary"),\
                                  max("salary").alias("maxsalary"),\
                                  avg("salary").alias("avgsalary")             
                                )
df1.display()

# COMMAND ----------

emp.csv 
-------
empid,empname,deptno
101,srinivas,10
102,phani,20
103,lakshmi,30
104,Ravi,10
105,krishna,50

dept.csv 
--------
deptid,deptname
10,sales
20,admin
30,marketing
40,networking

# COMMAND ----------

dept_path="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/dept.csv"
emp_path="dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp-1.csv"

# COMMAND ----------

e_df = spark.read.format("csv").option("header",True).load(emp_path)
d_df = spark.read.format("csv").option("header",True).load(dept_path)

# COMMAND ----------

e_df.display()

# COMMAND ----------

d_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "inner")

res_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "left")

res_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "right")

res_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "fullouter")

res_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "leftsemi")

res_df.display()

# COMMAND ----------

res_df = e_df.join(d_df , e_df.deptno == d_df.deptid , "leftanti")

res_df.display()

# COMMAND ----------

lst1 = [
       (1,'srinivas','male'),
       (2,'phani','male')
]

df1 = spark.createDataFrame(lst1, schema = ['id','name','gender'])

df1.display()

# COMMAND ----------

lst2 = [
       (2,'phani','male'),
        (3,'krishna','male')
]

df2 = spark.createDataFrame(lst2, schema = ['id','name','gender'])

df2.display()

# COMMAND ----------

#res_df = df1.union(df2)
res_df = df1.unionAll(df2)
res_df = res_df.distinct()
res_df.display()

# COMMAND ----------

lst1 = [
       (2,'male','phani'),
        (3,'male','krishna')
]

df3 = spark.createDataFrame(lst1, schema = ['id','gender','name'])

df3.display()

# COMMAND ----------

#res_df = df1.union(df3)
res_df = df1.unionByName(df3)
res_df.display()

# COMMAND ----------

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.display()

# COMMAND ----------

df1 = df.orderBy("salary")
df1.display()

# COMMAND ----------

df2 = df.orderBy(col("salary").desc())
df2.display()

# COMMAND ----------

   [2,1,3,4,6,9,8,5]
        
   [2,1,3,4],[6,9,8,5]

sort --> per partition level sort --> [1,2,3,4] , [5,6,8,9]   ---> [1,2,3,4,5,6,8,9]
orderBy --> entire dataset level sort 

# COMMAND ----------

data = [
    (("James","Anne","Smith"),["Python","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Python","C++"],"NY","F"),
    (("Julia","","Williams"),["Hadoop","Azure"],"OH","F"),
    (("Maria","Anne","Jones"),["Hadoop","Azure"],"NY","M"),
    (("Jen","Mary","Brown"),["Hadoop","Azure"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","Azure"],"OH","M")
 ]

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------
#(("James","Anne","Smith"),["Python","Scala","C++"],"OH","M")

cust_schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data,schema = cust_schema)

df.display()

# COMMAND ----------

df2=df.select("state","gender","name.firstname","name.middlename","name.lastname",df.languages[0],df.languages[1],df.languages[2])
df2.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC withColumn
# MAGIC withColumnRenamed
# MAGIC concat, when..otherwise
# MAGIC drop 
# MAGIC filter 
# MAGIC groupBy
# MAGIC joins 
# MAGIC union
