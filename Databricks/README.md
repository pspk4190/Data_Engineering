Databricks: 
Databricks is a cloud-based data analytics platform that provides an interactive 
workspace for data engineers, data scientists, and analysts to collaborate on big data 
and machine learning projects. It is built on Apache Spark and integrates well with 
Azure, AWS, and Google Cloud. 

Key Features of Databricks 
1. Unified Data Platform – Combines data engineering, data science, and machine 
learning. 
2. Apache Spark Optimization – Provides a managed Spark environment with 
auto-scaling and performance improvements. 
3. Multi-Cloud Support – Available on Azure (Azure Databricks), AWS 
(Databricks on AWS), and GCP. 
4. Notebooks – Supports Python, Scala, SQL, and R for interactive data processing. 
5. Delta Lake – An optimized storage layer that enhances data reliability and 
performance. 
6. Security & Compliance – Provides enterprise-grade security and compliance 
support. 
7. Collaborative Workspace – Allows multiple users to work together in shared 
notebooks. 

Databricks uses its own filesystem for data processing operation, called DBFS 
(Databricks File system). It is a Linux based file system and we will Unix commands for 
various data processing operations. 
Databricks File System (DBFS) 
DBFS is a distributed file system built into Databricks. It allows users to interact with 
files and directories in Databricks clusters as if they were a regular file system. 

Key Features of DBFS 
 Mountable Storage – Can mount cloud storage (Azure ADLS, AWS S3, Google 
Cloud Storage). 
 Hierarchical File Structure – Files are organized in directories. 
 Supports DiƯerent File Formats – CSV, JSON, Parquet, Delta, etc. 
 Access Control – Permissions for files and directories. 
 In Databricks, we will use dbutils (databricks utilities) to interact DBFS and for 
data processing operations. 