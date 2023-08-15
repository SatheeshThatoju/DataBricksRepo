# Databricks notebook source
df_Product = spark.read.format("csv").option("inferSchema",True).option("header",True).load("/mnt/landingdata/Product/")

# COMMAND ----------

df_products = df_Product.filter("ProductCategoryId = 6")

# COMMAND ----------

 df_products.write.mode("overwrite").format("delta").saveAsTable("Product",path = "/mnt/rawdata/Product/")

