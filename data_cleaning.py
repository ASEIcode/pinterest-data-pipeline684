# Databricks notebook source
# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/create_s3_dataframes"

# COMMAND ----------

# MAGIC %md # Cleaning df_pin

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
