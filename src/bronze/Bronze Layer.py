# Databricks notebook source
# MAGIC %md
# MAGIC ## Dynamic Capabilities

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Ingestion

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", f"abfss://bronze@databricksete10.dfs.core.windows.net/checkpoint_{p_file_name}")\
        .load(f"abfss://sourcedata@databricksete10.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.writeStream.format("parquet")\
    .option("checkpointLocation", f"abfss://bronze@databricksete10.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path",f"abfss://bronze@databricksete10.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()