# Databricks notebook source
# MAGIC %md # Data Cleaning Notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/batch_notebooks/create_dataframes_from_s3"

# COMMAND ----------

# MAGIC %md ## Cleaning df_pin

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Replace missing values with None, letters with numbers and removing extra text from save locations.
df_pin = df_pin.replace({"No description available": None, "No description available Story format": None, "Untitled": None}, subset=["description"])
df_pin = df_pin.replace({"Image src error.": None}, subset=["image_src"])
df_pin = df_pin.replace({"User Info Error": None}, subset=["poster_name"])
df_pin = df_pin.replace({"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e": None}, subset=["tag_list"])
df_pin = df_pin.replace({"No Title Data Available": None}, subset=["title"])
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
df_pin = df_pin.replace({"User Info Error": None}, subset=["follower_count"])
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# rename index column
df_pin = df_pin.withColumnRenamed("index", "ind")

# cast to corrrect datatypes
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))
df_pin = df_pin.withColumn("ind", df_pin["ind"].cast("int"))
df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("int"))

#reorder columns
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

# COMMAND ----------

display(df_pin)

# COMMAND ----------

# MAGIC %md ## Cleaning df_geo

# COMMAND ----------

from pyspark.sql.functions import array
from pyspark.sql.functions import to_timestamp

# add a coordinates column from an array of lat and long
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# cast the timestamp column to a timestamp dtype
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# reorder columns
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

display(df_geo)

# COMMAND ----------

# MAGIC %md ## Cleaning df_user

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# concatenate first_name and last_name into username and drop old columns
df_user = df_user.withColumn("user_name", concat("first_name", lit((" ")), "last_name"))
df_user = df_user.drop("first_name", "last_name")

# cast date_joined to timestamp dtype
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

# rename index to ind
df_user = df_user.withColumnRenamed("index", "ind")

# reorder columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

display(df_user)
