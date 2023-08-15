# Databricks notebook source
jdbcUsername = "sqladmin"
jdbcPassword = "Azure$2023"
jdbcHostname = "az-std-sql-test.database.windows.net"
jdbcDatabase = "test-sqldb"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
 
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
 

# COMMAND ----------

df_ProdCategory = spark.read.format("Delta").load("/mnt/curateddata/ProductCategories/")

# COMMAND ----------

df_ProdCategory.write.jdbc(url=jdbcUrl, table="dbo.ProductCategory", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

df_Prod = spark.read.format("Delta").load("/mnt/curateddata/Products/")

# COMMAND ----------

df_Prod.write.jdbc(url=jdbcUrl, table="dbo.Product", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

df_SalesOrder = spark.read.format("Delta").load("/mnt/curateddata/SalesOrderDetails/")

# COMMAND ----------

df_SalesOrder.write.jdbc(url=jdbcUrl, table="dbo.SalesOrderDetails", mode="overwrite", properties=connectionProperties)
