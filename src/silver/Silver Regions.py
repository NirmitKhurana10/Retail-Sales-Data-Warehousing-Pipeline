# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df= spark.read.table("databricks_cata.bronze.regions")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksete10.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksete10.dfs.core.windows.net/regions'