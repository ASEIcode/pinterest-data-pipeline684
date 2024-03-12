# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib



# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

def read_stream(stream_name: str):
    stream_df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', stream_name) \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()
    return stream_df

# COMMAND ----------

pin_stream_df = read_stream('streaming-0e95b18877fd-pin')
geo_stream_df = read_stream('streaming-0e95b18877fd-geo')
user_stream_df = read_stream('streaming-0e95b18877fd-user')

