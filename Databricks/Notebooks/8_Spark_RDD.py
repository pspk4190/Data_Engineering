# Databricks notebook source
lst=[1,2,3,4,5,6,7,8,9,10]
x=sc.parallelize(lst)
print(x.collect())

# COMMAND ----------

x.getNumPartitions()

# COMMAND ----------

#processing csv file
filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/emp.csv'
x=sc.textFile(filepath)
x.collect()

# COMMAND ----------

def getsqr(n):
    return n*n

print (getsqr(11))

# COMMAND ----------

n=lambda x:x*x
print(n(11))

# COMMAND ----------

def getsum(a,b):
    return a+b

print (getsum(11,12))

# COMMAND ----------

j=lambda x,y:x+y
print(j(11,12))

# COMMAND ----------

#map function
def getsqr(n):
    return n*n

lst=[1,2,3,4,5]
res=list(map(getsqr,lst))
print(res)


# COMMAND ----------

def getupper(str):
    return str.upper()

lst = ['apple', 'banana', 'mango', 'cherry']

res = list(map(getupper, lst))
print(res)

# COMMAND ----------

def iseven(n):
    if n%2==0:
        return True
    else:
        return False
lst=[10,11,12,13,14,15,16]

res=list(filter(iseven,lst))
print(res)

# COMMAND ----------

lst=[10,11,12,13,14,15,16]
x=list(filter(lambda x:x%2==0,lst))
print(x)

# COMMAND ----------

#RDD Transformations

lst=[10,20,30,40,50,60]

rdd1=sc.parallelize(lst)
rdd2=rdd1.map(lambda x:x+5)
rdd2.collect()

# COMMAND ----------

#Finding odd number
lst=[1,2,3,4,5,6]

rdd1=sc.parallelize(lst)
rdd2=rdd1.filter(lambda x:x%2==1)
rdd2.collect()

# COMMAND ----------

#flatmap -display values in a single line

lst=[1,2,3,4,5]

rdd1=sc.parallelize(lst)
rdd2=rdd1.flatMap(lambda x:(x,x*x))
rdd2.collect()

# COMMAND ----------

#Groupby

lst=['Anthony', 'Amar', 'Balu', 'Bhagavan', 'Chandu', 'Chakri']

rdd1=sc.parallelize(lst)
rdd2=rdd1.groupBy(lambda x:x[0])
res=rdd2.collect()
for i,j in res:
    print(i,list(j))
                

# COMMAND ----------

lst=[
    (1, 'aa'),
    (2, 'bb'),
    (1, 'aa'),
    (2, 'bb')
]

rdd1=sc.parallelize(lst)
rdd2=rdd1.reduceByKey(lambda x,y:x+y)
res=rdd2.collect()
print(res)

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/salary.txt')

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/salary.txt'
rdd1=sc.textFile(filepath)
rdd2=rdd1.map(lambda line:line.split(","))
rdd3=rdd2.map(lambda lst:(lst[2],int(lst[3])))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd4.collect()

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/salary.txt'
rdd1=sc.textFile(filepath)
rdd2=rdd1.map(lambda line:line.split(",")).map(lambda lst:(lst[2],int(lst[3]))).reduceByKey(lambda x,y:x+y)
rdd2.collect()

# COMMAND ----------

My name is Pavan
Pavan is a Data Engineer
Pavan is from Andhra

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/details.txt')

# COMMAND ----------

filepath='dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/details.txt'
rdd1=sc.textFile(filepath)
rdd2=rdd1.flatMap(lambda line:line.split(" "))
rdd3=rdd2.map(lambda word:(word,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd4.collect()
