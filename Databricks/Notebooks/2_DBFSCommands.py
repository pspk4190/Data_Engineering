# Databricks notebook source
#DBFS -- DataBricks File System --> It is a linux based file system and we will use linux commands for activities.
dbutils.fs.help()

# COMMAND ----------

'''
/FileStore/shared_uploads/kpavankumar335@gmail.com/
'''

# COMMAND ----------

#Creating a new inbound directory
#Syntax --> dbutils.fs.mkdirs('Filepath)
#mkdirs --> Is a linux command used to create new directory.
dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')
dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')
dbutils.fs.mkdirs('/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles')

# COMMAND ----------

#list files in a specific directory
#Syntax: dbutils.fs.ls('filesystem path')
#ls --> It is a command used to list all files in a dirctory.
dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

#copy files between inbound and outbound folders
#Syntax: dbutils.fs.cp(source path, target path)
#cp --> It is a command used to copy file between source and target.

dbutils.fs.cp(
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Customers.txt',
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound'
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound/')

# COMMAND ----------

#Copy all files from inbound to outbound using recursive
#By default the recursive option will set to false, we need to make true.
#Syntax --> dbutils.fs.cp('src path','target path',True)

dbutils.fs.cp(
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound',
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound', True
    )

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles')

# COMMAND ----------

#copy files between inbound and outbound folders
#Syntax: dbutils.fs.mv(source path, target path)
#mv --> It is a command used to move file between source and target.

dbutils.fs.mv(
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound/Customers.txt',
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles'
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

#Move all files from inbound to outbound using recursive
#By default the recursive option will set to false, we need to make true.
#Syntax --> dbutils.fs.mv('src path','target path',True)

dbutils.fs.mv(
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound',
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound', True
    )

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/inbound')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')

# COMMAND ----------

#to display the content of the file.
#syntax - dbutils.fs.head('file path)

dbutils.fs.head('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound/Customers.txt')

# COMMAND ----------

#to input content to the file
#syntax - dbutiils.fs.put('file path', 'content')
dbutils.fs.put('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound/Welcome.txt',
'Welcome to Databricks Community'
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')

# COMMAND ----------

#to delete the files
# syntax - dbutils.fs.rm('filepath')
dbutils.fs.rm('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound/Welcome.txt')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')

# COMMAND ----------

#Deletion of all files in a directory
#Syntax: dbutils.fs.rm(source path, recursive argument)

dbutils.fs.rm(
    '/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound',
    True
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/outbound')

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles')

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles/welcome.txt')

# COMMAND ----------

#to create new file and input content to the file
#syntax - dbutiils.fs.put('file path', 'content')
dbutils.fs.put('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles/welcome1.txt',
'Welcome to Databricks Community'
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles')

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/shared_uploads/kpavankumar335@gmail.com/srcfiles/welcome1.txt')
