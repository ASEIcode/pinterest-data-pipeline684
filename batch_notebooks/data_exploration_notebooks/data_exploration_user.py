# Databricks notebook source
# MAGIC %md # Data exploration df_user

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/batch_notebooks/create_dataframes_from_s3"

# COMMAND ----------

df = df_user

# COMMAND ----------

# MAGIC %md ## Tasks 
# MAGIC - Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC - Drop the first_name and last_name columns from the DataFrame
# MAGIC - Convert the date_joined column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - user_name
# MAGIC   - age
# MAGIC   - date_joined

# COMMAND ----------

# MAGIC %md ### Create username column from first and last names

# COMMAND ----------

from pyspark.sql.functions import concat, lit

df = df.withColumn("user_name", concat("first_name", lit((" ")), "last_name"))



# COMMAND ----------

# MAGIC %md ### Drop the first name and last name columns

# COMMAND ----------

df = df.drop("first_name", "last_name")

# COMMAND ----------

# MAGIC %md ### Cast date_joined to timestamp dtype

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df = df.withColumn("date_joined", to_timestamp("date_joined"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md ### Rename the index column to ind

# COMMAND ----------

df = df.withColumnRenamed("index", "ind")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reorder the DataFrame columns to have the following column order:
# MAGIC - ind
# MAGIC - user_name
# MAGIC - age
# MAGIC - date_joined

# COMMAND ----------

df = df.select("ind", "user_name", "age", "date_joined")
display(df)

# COMMAND ----------

# MAGIC %md ## Final code to be added to data_cleaning notebeook

# COMMAND ----------

from pyspark.sql.functions import concat, lit
from pyspark.sql.functions import to_timestamp

# concatenate first_name and last_name into username and drop old columns
df_user = df_user.withColumn("user_name", concat("first_name", lit((" ")), "last_name"))
df_user = df_user.drop("first_name", "last_name")

# cast date_joined to timestamp dtype
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# rename index to ind
d_user = df_user.withColumnRenamed("index", "ind")

# reorder columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")
