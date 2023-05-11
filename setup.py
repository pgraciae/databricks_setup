# Databricks notebook source
display(spark.sql("SHOW STORAGE CREDENTIALS"))

# COMMAND ----------

display(spark.sql("DESCRIBE STORAGE CREDENTIAL adlg2test01_cred"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION adlg2test01
# MAGIC URL 'abfss://metastoredb@adlg2test01.dfs.core.windows.net/datalake'
# MAGIC WITH (STORAGE CREDENTIAL adlg2test01_cred);

# COMMAND ----------

display(spark.sql("DESCRIBE EXTERNAL LOCATION adlg2test01"))
