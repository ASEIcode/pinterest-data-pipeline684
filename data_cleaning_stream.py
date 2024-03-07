# Databricks notebook source
# MAGIC %md # Data Stream Cleaning Notebook

# COMMAND ----------

# MAGIC %md ## Import data from read_stream notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/read_stream"

# COMMAND ----------

# MAGIC %md ## Deserialise stream data

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Convert the streams back to standard dataframes ready for transformations

# Convert pin data stream
df_pin_stream = pin_stream_df.selectExpr("CAST(data as STRING)")
pin_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])

df_pin_stream_parsed = df_pin_stream.withColumn("data", from_json("data", pin_schema))

df_pin = df_pin_stream_parsed.select(
    col("data.*")
)
# due to an error when I first set up the stream there are around 1000 rows with all null values.
df_pin =  df_pin.dropna(how='all')

# COMMAND ----------

# Convert geo data stream
df_geo_stream = geo_stream_df.selectExpr("CAST(data as STRING)")
geo_schema = StructType([
    StructField("country", StringType()),
    StructField("ind", IntegerType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("timestamp", StringType())
])

df_geo_stream_parsed = df_geo_stream.withColumn("data", from_json("data", geo_schema))

df_geo = df_geo_stream_parsed.select(
    col("data.*")
)
# due to an error when I first set up the stream there are around 1000 rows with all null values.
df_geo =  df_geo.dropna(how='all')

# COMMAND ----------

# Convert user data stream
df_user_stream = user_stream_df.selectExpr("CAST(data as STRING)")
user_schema = StructType([
    StructField("age", IntegerType()),
    StructField("date_joined", StringType()),
    StructField("first_name", StringType()),
    StructField("index", IntegerType()),
    StructField("last_name", StringType())
])

df_user_stream_parsed = df_user_stream.withColumn("data", from_json("data", user_schema))

df_user = df_user_stream_parsed.select(
    col("data.*")
)
# due to an error when I first set up the stream there are around 1000 rows with all null values.
df_user =  df_user.dropna(how='all')

# COMMAND ----------

# MAGIC %md ## Clean each stream

# COMMAND ----------

# MAGIC %md ### Cleaning df_pin

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

# MAGIC %md ### Cleaning df_geo

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

# MAGIC %md ### Cleaning df_user

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

# COMMAND ----------

# MAGIC %md ## Write to Delta Tables

# COMMAND ----------

def write_to_delta_table(dataframe: str, table_name: str):
    """
    Takes a dataframe and writes it to a delta table.
    Args:
        dataframe (dataframe): The dataframe to be written to the delta table
        table_name (str): The desired name for your new Delta table
    """
    dataframe.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
    .table(table_name)
    print(f"writing data to {table_name} (new delta table)")

# COMMAND ----------

write_to_delta_table(df_pin, "0e95b18877fd_pin_table")

# COMMAND ----------

write_to_delta_table(df_geo, "0e95b18877fd_geo_table")

# COMMAND ----------

write_to_delta_table(df_user, "0e95b18877fd_user_table")
