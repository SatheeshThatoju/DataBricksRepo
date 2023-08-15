# Databricks notebook source
df_Product_Category = spark.read.format("csv").option("inferSchema",True).option("header",True).load("/mnt/landingdata/ProductCategory/")

# COMMAND ----------

df_Product_Categories = df_Product_Category.filter("ProductCategoryID=6")

# COMMAND ----------

df_Product_Categories.write.mode("overwrite").format("delta").saveAsTable('ProductCategory',path="/mnt/rawdata/ProductCategory/")
