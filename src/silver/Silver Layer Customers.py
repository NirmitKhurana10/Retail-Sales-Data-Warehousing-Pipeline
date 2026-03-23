# Databricks notebook source
# MAGIC %md
# MAGIC ### **Imports**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@databricksete10.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation**

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# Transforming email into 2 different columns for domain specific "marketing campaign"

df = df.withColumn("domains",split(col("email"),"@")[1])

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers",ascending=False).display()

# COMMAND ----------

df = df.withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name")))
df = df.drop("first_name","last_name")

#all emails in lower case

df = df.withColumn("email",lower(col("email")))

# COMMAND ----------

# df_gmail = df.filter(col("domains") == "gmail.com")
# df_gmail.display()
# df_hotmail = df.filter(col("domains") == "hotmail.com")
# df_hotmail.display()
# df_yahoo = df.filter(col("domains") == "yahoo.com")
# df_yahoo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@databricksete10.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating Table on silver layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema databricks_cata.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.customers_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksete10.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.customers_silver

# COMMAND ----------

# MAGIC %md