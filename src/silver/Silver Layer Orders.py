# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@databricksete10.dfs.core.windows.net/orders")

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df = df.withColumn("year",year(col("order_date")))
df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format("delta").mode("append").save("abfss://silver@databricksete10.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rough Work to learn OOP concepts and ranking functions
# MAGIC

# COMMAND ----------

# Making a class

# class windows:

#     def dense_rank(self, df):
#         df = df.withColumn("dense_rank",dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
#         return df
    
#     def rank(self, df):
#         df = df.withColumn("rank",rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
#         return df  
    
#     def row_rank(self, df):
#         df = df.withColumn("row_rank",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
#         return df

# COMMAND ----------

# # Calling functions in class to use them in dataset
# w = windows()
# df1 = w.dense_rank(df)
# df1 = w.rank(df1)
# df1 = w.row_rank(df1)
# df1.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.orders_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksete10.dfs.core.windows.net/orders'