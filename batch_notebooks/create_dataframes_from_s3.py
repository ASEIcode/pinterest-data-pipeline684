# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/0e95b18877fd-S3/topics/0e95b18877fd.geo/partition=0/
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "dbfs:/mnt/0e95b18877fd-S3/topics/0e95b18877fd.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

file_location = "dbfs:/mnt/0e95b18877fd-S3/topics/0e95b18877fd.user/partition=0/*.json" 
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

file_location = "dbfs:/mnt/0e95b18877fd-S3/topics/0e95b18877fd.pin/partition=0/*.json" 
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

# COMMAND ----------

display(df_pin)

# COMMAND ----------

#checking all dataframes are uploaded and available
print("-------Describe .pin-------")
display(df_pin.describe())
print("-------Describe .geo-------")
display(df_geo.describe())
print("-------Describe .user-------")
display(df_user.describe())
