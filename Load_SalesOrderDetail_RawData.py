# Databricks notebook source
df_SalesOrderDetail = spark.read.format("csv").option("inferSchema",True).option("header",True).load("/mnt/landingdata/SalesOrderDetail/")

df_Product = spark.read.format("csv").option("inferSchema",True).option("header",True).load("/mnt/landingdata/Product/")

df_ProductCategory = spark.read.format("csv").option("inferSchema",True).option("header",True).load("/mnt/landingdata/ProductCategory/")

# COMMAND ----------

df_ProductCategory.filter(df_ProductCategory.ProductCategoryID == 6)

# COMMAND ----------

df_prod = df_Product.filter(df_Product.ProductCategoryID==6)

# COMMAND ----------

from pyspark.sql.functions import col

#sales order details alias
sod_alias = 'sod'

lc = [col(f"{sod_alias}.{i}") for i in df_SalesOrderDetail.columns]
lc


# COMMAND ----------

res_df = df_SalesOrderDetail.alias(sod_alias).join(df_prod,on=[df_SalesOrderDetail.ProductID == df_prod.ProductID]).select(lc)


# COMMAND ----------

 res_df.write.mode("overwrite").format("delta").saveAsTable("SalesOrderDetail", path = "/mnt/rawdata/SalesOrderDetail/")

