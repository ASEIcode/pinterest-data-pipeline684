# Databricks notebook source
# MAGIC %run /Workspace/Repos/adamevansjs@gmail.com/pinterest-data-pipeline684/data_cleaning

# COMMAND ----------

# MAGIC %md # Queries

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Contents:
# MAGIC
# MAGIC - Find the most popular Pinterest category people post to based on their country.
# MAGIC
# MAGIC - Find which was the most popular category each year
# MAGIC
# MAGIC - Find the user with most followers in each country
# MAGIC
# MAGIC - Find the most popular category for different age groups
# MAGIC
# MAGIC - Find how many users have joined each year
# MAGIC
# MAGIC - Find the median follower count of users based on their joining year
# MAGIC
# MAGIC - Find the median follower count of users based on their joining year and age group
# MAGIC

# COMMAND ----------

# MAGIC %md ### Find the most popular Pinterest category people post to based on their country

# COMMAND ----------

print("---df_pin table---")
df_pin.printSchema()
print("---df_geo table---")
df_geo.printSchema()
print("---df_user table---")
df_user.printSchema()

# COMMAND ----------

df_pin.createOrReplaceTempView("df_pin")
df_geo.createOrReplaceTempView("df_geo")
df_user.createOrReplaceTempView("df_user")

# COMMAND ----------

most_popular_cat_by_country_df = spark.sql("""
SELECT g.country, MAX(p.category) as category, COUNT(p.category) as category_count 
FROM df_pin as p 
JOIN df_geo as g ON p.ind = g.ind 
GROUP BY g.country
""")
display(most_popular_cat_by_country_df)

# COMMAND ----------

# MAGIC %md ### Find which was the most popular category each year

# COMMAND ----------

most_popular_cat_by_year_df = spark.sql("""
SELECT YEAR(g.timestamp) as post_year, MAX(p.category) as category, COUNT(p.category) as category_count
FROM df_pin as p 
JOIN df_geo as g ON p.ind = g.ind 
GROUP BY YEAR(g.timestamp)
SORT BY YEAR(g.timestamp)
""")
display(most_popular_cat_by_year_df)

# COMMAND ----------

# MAGIC %md ### Find the user with most followers in each country

# COMMAND ----------

most_followers_each_country_df = spark.sql("""
SELECT g.country, MAX(p.poster_name) as poster_name, MAX(p.follower_count) as follower_count
FROM df_pin as p 
JOIN df_geo as g ON p.ind = g.ind
GROUP BY g.country
""")
display(most_followers_each_country_df)

# COMMAND ----------

# MAGIC %md #### Based on the above query, find the country with the user with most followers

# COMMAND ----------

most_followers_each_country_df.createOrReplaceTempView("most_followers_each_country_df")
country_with_user_most_followers_df = spark.sql("""
SELECT country, follower_count 
FROM most_followers_each_country_df
SORT BY follower_count DESC
LIMIT 1
""")
display(country_with_user_most_followers_df)

# COMMAND ----------

# MAGIC %md ### What is the most popular category people post to based on the following age groups:
# MAGIC
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - age_group, a new column based on the original age column
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

age_groups_cat = spark.sql("""
SELECT p.category, u.age, 
    CASE
        WHEN AGE BETWEEN 18 AND 24 THEN '18-24'
        WHEN AGE BETWEEN 25 AND 35 THEN '25-35'
        WHEN AGE BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END AS age_group
FROM df_pin as p
JOIN df_user as u ON p.ind = u.ind
""")

age_groups_cat.createOrReplaceTempView("age_groups_cat")

popular_cat_by_age_group = spark.sql("""
SELECT age_group, max(category) as category, count(category) as category_count
FROM age_groups_cat
GROUP BY age_group 
""")
display(popular_cat_by_age_group)



# COMMAND ----------

# MAGIC %md ### What is the median follower count for users in the following age groups:
# MAGIC
# MAGIC - 18-24
# MAGIC - 25-35
# MAGIC - 36-50
# MAGIC - +50
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - age_group, a new column based on the original age column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

age_group_followers = spark.sql("""
SELECT p.follower_count, u.age, 
    CASE
        WHEN AGE BETWEEN 18 AND 24 THEN '18-24'
        WHEN AGE BETWEEN 25 AND 35 THEN '25-35'
        WHEN AGE BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END AS age_group
FROM df_pin as p
JOIN df_user as u ON p.ind = u.ind
""")
age_group_followers.createOrReplaceTempView("age_group_followers")

# COMMAND ----------

age_group_median_followers = spark.sql("""
SELECT age_group, percentile_approx(follower_count, 0.5) as median_follower_count
FROM age_group_followers
GROUP BY age_group
ORDER BY age_group
""")

display(age_group_median_followers)

# COMMAND ----------

# MAGIC %md ### Find how many users have joined between 2015 and 2020.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - number_users_joined, a new column containing the desired query output

# COMMAND ----------

users_each_year = spark.sql("""
SELECT YEAR(g.timestamp) as post_year, COUNT(u.date_joined) as number_users_joined
FROM df_geo as g
JOIN df_user as u
ON g.ind = u.ind
WHERE YEAR(g.timestamp) BETWEEN 2015 AND 2020
GROUP BY YEAR(g.timestamp)
SORT BY YEAR(g.timestamp)
""")
display(users_each_year)

# COMMAND ----------

# MAGIC %md ### Find the median follower count of users have joined between 2015 and 2020.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output 

# COMMAND ----------

post_year_median_followers = spark.sql("""
SELECT YEAR(g.timestamp) as post_year, percentile_approx(follower_count, 0.5) as median_follower_count
FROM df_pin as p
JOIN df_geo as g ON p.ind = g.ind
WHERE YEAR(g.timestamp) BETWEEN 2015 AND 2020
GROUP BY YEAR(g.timestamp)
ORDER BY YEAR(g.timestamp)
""")

display(post_year_median_followers)

# COMMAND ----------

# MAGIC %md ### Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - age_group, a new column based on the original age column
# MAGIC - post_year, a new column that contains only the year from the timestamp column
# MAGIC - median_follower_count, a new column containing the desired query output

# COMMAND ----------

followers_age_group_year_joined = spark.sql("""
SELECT p.follower_count, YEAR(g.timestamp) AS post_year, YEAR(u.date_joined) AS date_joined_year, u.age,
    CASE
        WHEN AGE BETWEEN 18 AND 24 THEN '18-24'
        WHEN AGE BETWEEN 25 AND 35 THEN '25-35'
        WHEN AGE BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+'
    END AS age_group
FROM df_pin AS p
JOIN df_user AS u ON p.ind = u.ind
JOIN df_geo AS g ON u.ind = g.ind 
""")
followers_age_group_year_joined.createOrReplaceTempView("followers_age_group_year_joined")

followers_by_age_group_2015_2020 = spark.sql("""
SELECT age_group, post_year as post_year, percentile_approx(follower_count, 0.5) as median_follower_count
FROM followers_age_group_year_joined
WHERE post_year BETWEEN 2015 AND 2020
GROUP BY age_group, post_year
SORT BY post_year, age_group
""")
display(followers_by_age_group_2015_2020)
