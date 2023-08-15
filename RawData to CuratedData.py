# Databricks notebook source
# MAGIC %fs ls /mnt/rawdata

# COMMAND ----------

df_ProductCategory_Curated = spark.read.format("delta").load("/mnt/rawdata/ProductCategory/")

# COMMAND ----------

df_ProductCategories_Curated = df_ProductCategory_Curated.filter("ProductCategoryId=6").select("ProductCategoryID","Name")

# COMMAND ----------

df_ProductCategories_Curated.display()

# COMMAND ----------

df_ProductCategories_Curated.write.mode("overwrite").format("delta").saveAsTable('ProductCategories',path="/mnt/curateddata/ProductCategories")

# COMMAND ----------

spark.read.format("Delta").load("/mnt/curateddata/ProductCategories/").display()

# COMMAND ----------

df_Product_Curated = spark.read.format("delta").load("/mnt/rawdata/Product/")

# COMMAND ----------

df_Products_Curated = df_Product_Curated.filter("ProductCategoryId=6").select("ProductID","Name","StandardCost","ListPrice","ProductCategoryID")

# COMMAND ----------

df_Products_Curated.write.mode("overwrite").format("delta").saveAsTable('Products',path="/mnt/curateddata/Products")

# COMMAND ----------

spark.read.format("Delta").load("/mnt/curateddata/Products/").display()

# COMMAND ----------

df_SalesOrderDetail = spark.read.format("Delta").load("/mnt/rawdata/SalesOrderDetail/")

df_Product = spark.read.format("Delta").load("/mnt/rawdata/Product/")

df_ProductCategory = spark.read.format("Delta").load("/mnt/rawdata/ProductCategory/")

# COMMAND ----------

df_ProductCategory.filter(df_ProductCategory.ProductCategoryID == 6).display()

# COMMAND ----------

df_prod = df_Product.filter(df_Product.ProductCategoryID==6)

# COMMAND ----------

from pyspark.sql.functions import col

#sales order details alias
sod_alias = 'sod'

lc = [col(f"{sod_alias}.{i}") for i in df_SalesOrderDetail.columns]
lc


# COMMAND ----------

res_df = df_SalesOrderDetail.alias(sod_alias).join(df_prod,on=[df_SalesOrderDetail.ProductID == df_prod.ProductID]).select("sod.SalesOrderID", "sod.SalesOrderDetailID","sod.OrderQty","sod.ProductID","sod.UnitPrice","sod.UnitPriceDiscount","sod.LineTotal")
res_df.display()

# COMMAND ----------

 res_df.write.mode("overwrite").format("delta").saveAsTable("SalesOrderDetails", path = "/mnt/curateddata/SalesOrderDetails/")


# COMMAND ----------

spark.read.format("Delta").load("/mnt/curateddata/SalesOrderDetails/").display()

# COMMAND ----------


