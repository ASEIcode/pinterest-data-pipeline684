# Databricks notebook source
# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/batch_notebooks/create_dataframes_from_s3"

# COMMAND ----------

# MAGIC %md # df_geo data exploration

# COMMAND ----------

# MAGIC %md ## Tasks
# MAGIC
# MAGIC - Create a new column coordinates that contains an array based on the latitude and longitude columns
# MAGIC - Drop the latitude and longitude columns from the DataFrame
# MAGIC - Convert the timestamp column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - country
# MAGIC   - coordinates
# MAGIC   - timestamp

# COMMAND ----------

# copy the dataframe for testing
df = df_geo

# COMMAND ----------

# MAGIC %md ### Create a new column coordinates that contains an array based on the latitude and longitude columns then Drop the latitude and longitude columns from the DataFrame

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import array

display(df.withColumn("coordinates", array("latitude", "longitude")))

# COMMAND ----------

df = df.withColumn("coordinates", array("latitude", "longitude"))
df = df.drop("latitude", "longitude")
display(df)

# COMMAND ----------

# MAGIC %md ### Convert the timestamp column from a string to a timestamp data type

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df = df.withColumn("timestamp", to_timestamp("timestamp"))

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md ### Reorder the DataFrame columns to have the following column order:
# MAGIC - ind
# MAGIC - country
# MAGIC - coordinates
# MAGIC - timestamp

# COMMAND ----------

df = df.select("ind", "country", "coordinates", "timestamp")
display(df)

# COMMAND ----------

# MAGIC %md ## Final code to be pasted into data_cleaning notebook

# COMMAND ----------

from pyspark.sql.functions import array
from pyspark.sql.functions import to_timestamp

# add a coordinates column from an array of lat and long
df_geo.withColumn("coordinates", array("latitude", "longitude"))

# cast the timestamp column to a timestamp dtype
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# reorder the columns
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")
