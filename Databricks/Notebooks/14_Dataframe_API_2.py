# Databricks notebook source
data = [(1, "John", None),
        (2, "Alice", 25),
        (3, None, 30)]

df = spark.createDataFrame(data, ["id", "name", "age"])

df.display()

# COMMAND ----------

df1 = df.dropna()

df1.display()

# COMMAND ----------

data = [(1, "John", None),
        (2, "Alice", 25),
        (3, None, 30),
        (None,None,None)
        ]

df = spark.createDataFrame(data, ["id", "name", "age"])

df.display()

# COMMAND ----------

df1= df.dropna('all')
df1.display()

# COMMAND ----------

df1 = df.fillna({'name':'UNKNOWN','age':0})
df1.display()

# COMMAND ----------

sql --> window functions 
denserank
rank
rownumber
lag 
lead

# COMMAND ----------

emp_windows.csv
----------------
empid,empname,gender,deptno,sal
121,rajendra,M,10,50000
111,Krishna,M,10,40000
101,Prasad,M,10,25000
106,viswas,M,10,25000
116,kishore,M,10,25000
107,akhila,F,20,45000
102,Rahul,M,20,30000
117,Sravani,F,20,30000
112,ahmad,M,20,20000
103,aditya,M,30,40000
118,sivakrishna,M,30,40000
108,prajol,M,30,35000
113,Anusha,F,30,30000
109,venu,M,40,40000
114,prasanth,M,40,40000
104,Pavani,F,40,20000
119,satish,M,40,20000
105,anupama,F,50,35000
120,Sunitha,F,50,25000
110,Sujatha,F,50,20000
115,aravind,M,50,20000

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_windows.csv

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_windows.csv')

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

emp_schema=StructType(
    [
        StructField("empid",IntegerType()),
        StructField("empname",StringType()),
        StructField("gender",StringType()),
        StructField("deptno",IntegerType()),
        StructField("sal",IntegerType()),
    ]
)

# COMMAND ----------

df=spark.read.format("csv").option("header",True).schema(emp_schema).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/emp_windows.csv")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("emp_v")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from
# MAGIC (select 
# MAGIC e.*,
# MAGIC dense_rank() over(order by sal desc) as drnk,
# MAGIC rank() over(order by sal desc) as rnk,
# MAGIC row_number() over(order by sal desc) as rnum
# MAGIC from emp_v e
# MAGIC )A
# MAGIC where A.drnk==2;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df1 = df.withColumn("drnk",dense_rank().over(Window.orderBy(col("sal").desc())))
df1 = df1.withColumn("rnk",rank().over(Window.orderBy(col("sal").desc())))
df1 = df1.withColumn("rnum",row_number().over(Window.orderBy(col("sal").desc())))

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC e.*,
# MAGIC dense_rank() over(partition by deptno order by sal desc) as drnk,
# MAGIC rank() over(partition by deptno order by sal desc) as rnk,
# MAGIC row_number() over(partition by deptno order by sal desc) as rnum
# MAGIC from emp_v e

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select
# MAGIC e.*,
# MAGIC dense_rank() over(partition by deptno order by sal desc) as drnk
# MAGIC from emp_v e
# MAGIC )A where A.drnk = 2

# COMMAND ----------

df1 = df.withColumn("drnk",dense_rank().over(Window.partitionBy("deptno").orderBy(col("sal").desc())))
df1 = df1.filter(df1.drnk==2)
df1.display()  

# COMMAND ----------

dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/year_quarter_sale.csv

# COMMAND ----------

cust_schema=StructType(
    [
        StructField("year",IntegerType()),
        StructField("quarter",IntegerType()),
        StructField("Sale_amt",IntegerType())
    ]
)

# COMMAND ----------

df2=spark.read.format("csv").option("header",True).schema(cust_schema).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/year_quarter_sale.csv")
df2.display()

# COMMAND ----------

df2.createOrReplaceTempView("emp_w")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from emp_w

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC year,
# MAGIC quarter,
# MAGIC Sale_amt,
# MAGIC lag(Sale_amt,1,0) over( partition by year order by year,quarter) as prv_qtr_sale
# MAGIC from emp_w

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC year,
# MAGIC quarter,
# MAGIC Sale_amt,
# MAGIC lag(Sale_amt,1,0) over( partition by year order by year,quarter) as prv_qtr_sale,
# MAGIC Sale_amt-prv_qtr_sale as diff,
# MAGIC lead(Sale_amt,1,0) over( partition by year order by year,quarter) as nxt_qtr_sale
# MAGIC from emp_w

# COMMAND ----------

# MAGIC %sql
# MAGIC select year, quarter,Sale_amt from 
# MAGIC (
# MAGIC select 
# MAGIC e.*,
# MAGIC lag(Sale_amt,1) over( partition by year order by year,quarter) as prv_qtr_sale
# MAGIC from emp_w e
# MAGIC )A where A.Sale_amt - A.prv_qtr_sale > 0

# COMMAND ----------

df2.display()

# COMMAND ----------

df2=spark.read.format("csv").option("header",True).schema(cust_schema).load("dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/emp_data/year_quarter_sale.csv")
df2 = df2.withColumn("prv_qtr_sale",lag(df2.Sale_amt).over(Window.partitionBy("year").orderBy("year","quarter")))
df2 = df2.filter(df2.Sale_amt - df2.prv_qtr_sale > 0)
df2.display()
