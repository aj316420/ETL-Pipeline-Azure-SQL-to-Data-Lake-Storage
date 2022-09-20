# Databricks notebook source
# MAGIC %md
# MAGIC ###<li> Extract data from Azure SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Establish JDBC connection

# COMMAND ----------

jdbcHostname = "ss-test123.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database-etlpipeline"
jdbcUsername = "testadmin"
jdbcPassword = "Pass@123"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcURL = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract product table

# COMMAND ----------

df_product = spark.read.format("jdbc").option("url", jdbcURL).option("dbtable", "SalesLT.Product").load()
display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract sales table

# COMMAND ----------

df_sales = spark.read.format("jdbc").option("url", jdbcURL).option("dbtable", "SalesLT.SalesOrderDetail").load()
display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ###<li> Transform data as per business requirement

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleansing dimension table by replacing null values

# COMMAND ----------

df_product_cleansed = df_product.na.fill({"Size": "M", "Weight": 100})
display(df_product_cleansed) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleansing fact table by drop duplicate records

# COMMAND ----------

df_sales_cleansed = df_sales.dropDuplicates()
display(df_sales_cleansed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join fact and dimension table

# COMMAND ----------

df_join = df_sales_cleansed.join(df_product_cleansed,df_sales_cleansed.ProductID ==df_product_cleansed.ProductID,"leftouter").select(df_sales_cleansed.ProductID, df_sales_cleansed.UnitPrice, df_sales_cleansed.LineTotal, df_product_cleansed.Name, df_product_cleansed.Color, df_product_cleansed.Size, df_product_cleansed.Weight)
display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation on joined table

# COMMAND ----------

df_agg = df_join.groupBy(["ProductID","Name","Color","Size","Weight"]).sum("LineTotal").withColumnRenamed("sum(LineTotal)","sum_total_sales")
display(df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ###<li> Load transformed data into Azure Data Lake Storage

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create mount point for Azure Data Lake Storage integration

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://container-etlpipeline@adlsetlpipeline.blob.core.windows.net", mount_point = "/mnt/etlpipeline_test", extra_configs = {"fs.azure.account.key.adlsetlpipeline.blob.core.windows.net":"q7/ukvC/3w04mXP3QpD+x2Ql6nMNk/Pu32JcJd/p08QBo60WxGCQBMTcwpwcb0I2+4LfnTmxvuA4+AStySLfkw=="})

# COMMAND ----------

# MAGIC %md
# MAGIC #### List files under mount point

# COMMAND ----------

dbutils.fs.ls("/mnt/etlpipeline_test")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data in Data Lake Storage

# COMMAND ----------

df_agg.write.format("delta").save("/mnt/etlpipeline_test/advn_work_delta/")

# COMMAND ----------

df_agg.write.format("csv").option("header","true").save("/mnt/etlpipeline_test/advn_work_csv/")
