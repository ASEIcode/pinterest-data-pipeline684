# Databricks notebook source
# MAGIC %run "/Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/batch_notebooks/create_dataframes_from_s3"

# COMMAND ----------

display(df_pin)

# COMMAND ----------

# MAGIC %md ## To Do List:

# COMMAND ----------

# MAGIC %md
# MAGIC This list was addeded to as I explore the data to make a transformation plan.
# MAGIC
# MAGIC - Replace empty entries and entries with no relevant data in each column with Nones
# MAGIC   - Run value counts and explore each column(add tasks to each section based on results):
# MAGIC     - ~~category~~
# MAGIC     - ~~description~~
# MAGIC       - 12 instances of "No description available Story format" > change to None
# MAGIC       - 1 instance of "5,237 points • 127 comments". Im not sure if this has been put in the wrong data field / table
# MAGIC       - 1 instance of "Untitled"
# MAGIC       - 2 instances of "No description available" > change to None
# MAGIC     - ~~downloaded~~
# MAGIC     - ~~follower_count~~
# MAGIC       - See the follower count section below this one
# MAGIC     - ~~image_src~~
# MAGIC       - 4 x "Image src error."
# MAGIC     - ~~index~~
# MAGIC     - ~~is_image_or_video~~
# MAGIC     - ~~poster_name~~
# MAGIC       - 3 x "User Info Error"
# MAGIC     - ~~save_location~~
# MAGIC     - ~~tag_list~~
# MAGIC       - 6 instances of "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"
# MAGIC     - ~~title~~
# MAGIC       - 3 instances of "No Title Data Available"
# MAGIC     - ~~unique_id~~
# MAGIC
# MAGIC - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# MAGIC
# MAGIC   - "User Info Error" replaced with null (None)
# MAGIC   - M replaced with "000000"
# MAGIC   - K replaced with 000
# MAGIC   - The cast to the correct dtype
# MAGIC
# MAGIC - Ensure that each column containing numeric data has a numeric data type
# MAGIC   - index
# MAGIC   - follower_count
# MAGIC   - downloaded
# MAGIC
# MAGIC - Clean the data in the save_location column to include only the save location path
# MAGIC   - Replace "Local save in" with ""
# MAGIC - Rename the index column to ind.
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - unique_id
# MAGIC   - title
# MAGIC   - description
# MAGIC   - follower_count
# MAGIC   - poster_name
# MAGIC   - tag_list
# MAGIC   - is_image_or_video
# MAGIC   - image_src
# MAGIC   - save_location
# MAGIC   - category
# MAGIC
# MAGIC   ALL COMPLETED - See Tasks in dataframe cleaning tests section

# COMMAND ----------

# MAGIC %md ## Data exploration:

# COMMAND ----------

# MAGIC %md ### Category column:

# COMMAND ----------

display(df_pin.groupby("category").count())

# COMMAND ----------

# MAGIC %md All is well with the category column.
# MAGIC
# MAGIC ## Description column:

# COMMAND ----------

display(df_pin.groupby("description").count())

# COMMAND ----------

# MAGIC %md  - 12 instances of "No description available Story format" > change to None
# MAGIC - 1 instance of "5,237 points • 127 comments". Im not sure if this has been put in the wrong data field / table
# MAGIC - 1 instance of "Untitled"
# MAGIC - 2 instances of "No description available" > change to None

# COMMAND ----------

# MAGIC %md ### Downloaded column:

# COMMAND ----------

display(df_pin.groupby("downloaded").count())

# COMMAND ----------

# MAGIC %md Each of the four "0" entries are the Story posts which have no data for many columns. Im guessing this intentional based on the format of this kind of post.

# COMMAND ----------

# MAGIC %md ### follower_count

# COMMAND ----------

display(df_pin.groupBy("follower_count").count() )

# COMMAND ----------

# MAGIC %md
# MAGIC The follower_count column (above) will need the following fixed to be cast as INT dtype:
# MAGIC
# MAGIC - "User Info Error" replaced with null (None)
# MAGIC - M replaced with "000000"
# MAGIC - K replaced with 000
# MAGIC - The cast to the correct dtype

# COMMAND ----------

# MAGIC %md ### image_src:

# COMMAND ----------

display(df_pin.groupBy("image_src").count() )

# COMMAND ----------

# MAGIC %md 4 x "Image src error."

# COMMAND ----------

# MAGIC %md ### index column:

# COMMAND ----------

display(df_pin.groupBy("index").count())

# COMMAND ----------

# MAGIC %md All unique, no nulls. All good. 

# COMMAND ----------

# MAGIC %md ### is_image_or_video column:

# COMMAND ----------

display(df_pin.groupBy("is_image_or_video").count())

# COMMAND ----------

# MAGIC %md no issues here

# COMMAND ----------

# MAGIC %md ### Poster_name column:

# COMMAND ----------

display(df_pin.groupBy("poster_name").count())

# COMMAND ----------

# MAGIC %md 3 x "User Info Error"

# COMMAND ----------

# MAGIC %md ### save_location column:

# COMMAND ----------

display(df_pin.groupBy("save_location").count())

# COMMAND ----------

# MAGIC %md No issues

# COMMAND ----------

# MAGIC %md ### tag_list column

# COMMAND ----------

display(df_pin.groupBy("tag_list").count())

# COMMAND ----------

- 6 instances of "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"

# COMMAND ----------

# MAGIC %md ### title column:

# COMMAND ----------

display(df_pin.groupBy("title").count())

# COMMAND ----------

# MAGIC %md - 3 instances of "No Title Data Available"

# COMMAND ----------

# MAGIC %md ### unique_id

# COMMAND ----------

display(df_pin.groupBy("unique_id").count())

# COMMAND ----------

# MAGIC %md 178 rows, no nulls, all in the same format. No issues.

# COMMAND ----------

# MAGIC %md ## Dataframe cleaning tests

# COMMAND ----------

# MAGIC %md ### Tasks
# MAGIC (copied from above for quick reference)
# MAGIC
# MAGIC - ~~Replace empty entries and entries with no relevant data in each column with Nones~~
# MAGIC
# MAGIC  - ~~description~~
# MAGIC     - ~~12 instances of "No description available Story format" > change to None~~
# MAGIC     - 1 instance of "5,237 points • 127 comments". Im not sure if this has been put in the wrong data field / table
# MAGIC     - ~~1 instance of "Untitled"~~
# MAGIC     - ~~2 instances of "No description available" > change to None~~
# MAGIC
# MAGIC - ~~image_src~~
# MAGIC     - ~~4 x "Image src error."~~
# MAGIC
# MAGIC  - ~~poster_name~~
# MAGIC     - ~~3 x "User Info Error"~~
# MAGIC
# MAGIC   - ~~tag_list~~
# MAGIC     - ~~6 instances of "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"~~
# MAGIC   - ~~title~~
# MAGIC     - ~~3 instances of "No Title Data Available"~~
# MAGIC
# MAGIC - ~~For follower_count~~
# MAGIC
# MAGIC     - ~~"User Info Error" replaced with null (None)~~
# MAGIC     - ~~M replaced with "000000"~~
# MAGIC     - ~~K replaced with 000~~
# MAGIC
# MAGIC - ~~Ensure that each column containing numeric data has a numeric data type~~
# MAGIC
# MAGIC     - ~~index~~
# MAGIC     - ~~follower_count~~
# MAGIC     - ~~downloaded~~
# MAGIC
# MAGIC - ~~For save_location:~~
# MAGIC   - ~~Replace "Local save in" with ""~~
# MAGIC   - ~~Rename the index column to ind.~~
# MAGIC
# MAGIC - ~~Reorder the DataFrame columns to have the following column order:~~
# MAGIC
# MAGIC   - ~~ind~~
# MAGIC   - ~~unique_id~~
# MAGIC   - ~~title~~
# MAGIC   - ~~description~~
# MAGIC   - ~~follower_count~~
# MAGIC   - ~~poster_name~~
# MAGIC   - ~~tag_list~~
# MAGIC   - ~~is_image_or_video~~
# MAGIC   - ~~image_src~~
# MAGIC   - ~~save_location~~
# MAGIC   - ~~category~~

# COMMAND ----------

# MAGIC %md Copy the dataframe to make it easy to reset if a produces errors

# COMMAND ----------

df_test = df_pin

# COMMAND ----------

# MAGIC %md #### Description column. Replace values with None

# COMMAND ----------

df_test = df_test.replace({"No description available": None, "No description available Story format": None, "Untitled": None}, subset=["description"])

# COMMAND ----------

display(df_test.filter("description IS NULL"))

# COMMAND ----------

# MAGIC %md Success. The Null count matches the amount of values we had to replace.

# COMMAND ----------

# MAGIC %md #### image_src - 4 x "Image src error."

# COMMAND ----------

df_test = df_test.replace({"Image src error.": None}, subset=["image_src"])
display(df_test.filter("image_src IS NULL"))

# COMMAND ----------

# MAGIC %md 4 x Null. Correct

# COMMAND ----------

# MAGIC %md  #### poster_name - 3 x "User Info Error"

# COMMAND ----------

df_test = df_test.replace({"User Info Error": None}, subset=["poster_name"])
display(df_test.filter("poster_name IS NULL"))

# COMMAND ----------

# MAGIC %md correct amount of null values.

# COMMAND ----------

# MAGIC %md #### tag_list -6 instances of "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"

# COMMAND ----------

df_test = df_test.replace({"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e": None}, subset=["tag_list"])
display(df_test.filter("tag_list IS NULL"))

# COMMAND ----------

# MAGIC %md 6 nulls. Matches.

# COMMAND ----------

# MAGIC %md #### title - 3 instances of "No Title Data Available"

# COMMAND ----------

df_test = df_test.replace({"No Title Data Available": None}, subset=["title"])
display(df_test.filter("title IS NULL"))

# COMMAND ----------

# MAGIC %md 3 Nulls. All good. 

# COMMAND ----------

# MAGIC %md #### follower_count value replacement
# MAGIC
# MAGIC     - "User Info Error" replaced with null (None) - 3 to replace
# MAGIC     - M replaced with "000000"
# MAGIC     - K replaced with 000

# COMMAND ----------

df_test = df_test.replace({"User Info Error": None, "M": "000000", "k": "000"}, subset=["follower_count"])

# COMMAND ----------

# MAGIC %md check for the 3 null values

# COMMAND ----------

display(df_test.filter("follower_count IS NULL"))

# COMMAND ----------

display(df_test.groupby("follower_count").count())

# COMMAND ----------

# MAGIC %md This hasnt worked for the K and M endings. Ill need to use a difference method.

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

display(df_test.withColumn("follower_count", regexp_replace("follower_count", "M", "000000")))

# COMMAND ----------

# MAGIC %md This method works. So the correct code block to replace values in follower_count will be:

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_test = df_test.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_test = df_test.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
df_test = df_test.replace({"User Info Error": None}, subset=["follower_count"])

# COMMAND ----------

display(df_test.groupby("follower_count").count())

# COMMAND ----------

# MAGIC %md This appears to have worked there are no letters. We can confirm this by attempting to cast the column to an integer.

# COMMAND ----------

df_test = df_test.withColumn("follower_count", df_test["follower_count"].cast("int"))

# COMMAND ----------

# check the dtype has changed
df_test.printSchema()

# COMMAND ----------

# MAGIC %md The printSchema() confirms that cast("int") was successful.

# COMMAND ----------

# MAGIC %md #### save_location / rename index col:
# MAGIC   - Replace "Local save in " with ""
# MAGIC   - Rename the index column to ind.

# COMMAND ----------

df_test = df_test.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
df_test = df_test.withColumnRenamed("index", "ind")
display(df_test.select("save_location", "ind"))

# COMMAND ----------

# MAGIC %md #### Cast numeric data types
# MAGIC
# MAGIC     - ind
# MAGIC     - downloaded

# COMMAND ----------

df_test = df_test.withColumn("ind", df_test["ind"].cast("int"))
df_test = df_test.withColumn("downloaded", df_test["downloaded"].cast("int"))
display(df_test.printSchema())

# COMMAND ----------

# MAGIC %md #### Reorder the DataFrame columns to have the following column order:
# MAGIC
# MAGIC   - ind
# MAGIC   - unique_id
# MAGIC   - title
# MAGIC   - description
# MAGIC   - follower_count
# MAGIC   - poster_name
# MAGIC   - tag_list
# MAGIC   - is_image_or_video
# MAGIC   - image_src
# MAGIC   - save_location
# MAGIC   - category

# COMMAND ----------

print("----------BEFORE--------------")
display(df_test)
df_test = df_test.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
print("-----------AFTER---------------")
display(df_test)

# COMMAND ----------

# MAGIC %md ## Final code to go into the datacleaning notebook

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


