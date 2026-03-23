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

df = spark.read.format("parquet").load("abfss://bronze@databricksete10.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformations**

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- function to return price after tax
# MAGIC
# MAGIC create or replace function databricks_cata.bronze.tax_func(prod_price double) 
# MAGIC returns double
# MAGIC language sql
# MAGIC return round(prod_price * 1.13,2);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_id, price as price_before_tax, databricks_cata.bronze.tax_func(price) as price_after_tax
# MAGIC from products

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.upper_func(p_brand string) 
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC   return p_brand.upper()
# MAGIC $$ 

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
        .option("path","abfss://silver@databricksete10.dfs.core.windows.net/products")\
            .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.products_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databricksete10.dfs.core.windows.net/products'