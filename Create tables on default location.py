# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo Unity catalog objects

# COMMAND ----------

# MAGIC %md
# MAGIC ![Securable objects](https://learn.microsoft.com/en-us/azure/databricks/_static/images/unity-catalog/object-hierarchy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC * METASTORE: The top-level container for metadata. Each Unity Catalog metastore exposes a three-level namespace (catalog.schema.table) that organizes your data.
# MAGIC * CATALOG: The first layer of the object hierarchy, used to organize your data assets.
# MAGIC * SCHEMA: Also known as databases, schemas are the second layer of the object hierarchy and contain tables and views.
# MAGIC * TABLE: The lowest level in the object hierarchy, tables can be external (stored in external locations in your cloud storage of choice) or managed tables (stored in a storage container in your cloud storage that you create expressly for Azure Databricks).
# MAGIC * VIEW: A read-only object created from one or more tables that is contained within a schema.
# MAGIC * EXTERNAL LOCATION: An object that contains a reference to a storage credential and a cloud storage path that is contained within a Unity Catalog metastore.
# MAGIC * STORAGE CREDENTIAL: An object that encapsulates a long-term cloud credential that provides access to cloud storage that is contained within a Unity Catalog metastore.
# MAGIC * FUNCTION: A user-defined function that is contained within a schema.
# MAGIC * SHARE: A logical grouping for the tables you intend to share using Delta Sharing. A share is contained within a Unity Catalog metastore.
# MAGIC * RECIPIENT: An object that identifies an organization or group of users that can have data shared with them using Delta Sharing. These objects are contained within a Unity Catalog metastore.
# MAGIC * PROVIDER: An object that represents an organization that has made data available for sharing using Delta Sharing. These objects are contained within a Unity Catalog metastore.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC For this exercise we already have configured a metastore (abfss://metastoredb@adlg2test01.dfs.core.windows.net/metastore) where all unity catalog objects and managed tables are going to be stored.
# MAGIC
# MAGIC A storage credential and a external location have been configured too.
# MAGIC
# MAGIC intial csv file exists in azure datalake

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### storage credential

# COMMAND ----------

display(spark.sql('SHOW STORAGE CREDENTIALS'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####External location where external tables will be stored

# COMMAND ----------

display(spark.sql('SHOW EXTERNAL LOCATIONS'))

# COMMAND ----------

# MAGIC %md
# MAGIC A csv file is loaded

# COMMAND ----------

externalLoaction = 'abfss://metastoredb@adlg2test01.dfs.core.windows.net/datalake'
default_file = dbutils.fs.ls(f'{externalLoaction}/initial_data')[0].path
print(default_file)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create [catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-catalogs) on default 

# COMMAND ----------

# For not default managed location.
# spark.sql("CREATE CATALOG [ IF NOT EXISTS ] <catalog_name> [ MANAGED LOCATION '<location_path>' ] [ COMMENT <comment> ]")

# COMMAND ----------

spark.sql('CREATE CATALOG IF NOT EXISTS testcatalog')
#managed tables will be stored in default location abfss://metastoredb@adlg2test01.dfs.core.windows.net/metastore

# COMMAND ----------

display(spark.sql('SHOW CATALOGS'))

# COMMAND ----------

# MAGIC %md
# MAGIC At this point we could assign and revoke privileges to the catalog
# MAGIC
# MAGIC [+info](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### [Create Schemas (databases)](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-schemas)

# COMMAND ----------

spark.sql('USE CATALOG testcatalog')
spark.sql("CREATE SCHEMA IF NOT EXISTS testschema "\
    "COMMENT 'test schema'")

# COMMAND ----------

display(spark.sql('SHOW SCHEMAS'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create managed table
# MAGIC
# MAGIC Managed tables are the default way to create tables in Unity Catalog. Unity Catalog manages the lifecycle and file layout for these tables. You should not use tools outside of Azure Databricks to manipulate files in these tables directly.
# MAGIC
# MAGIC By default, managed tables are stored in the root storage location that you configure when you create a metastore. You can optionally specify managed table storage locations at the catalog or schema levels, overriding the root storage location. Managed tables always use the Delta table format.
# MAGIC
# MAGIC When a managed table is dropped, its underlying data is deleted from your cloud tenant within 30 days.

# COMMAND ----------

# Read external file in data lake
options = {
    'header':True,
    'inferSchema': True,
    'delimiter': ','
}
df = spark.read.format('csv').options(**options).load(default_file)
display(df.take(10))

# COMMAND ----------

#save file as table in managed location
spark.sql('USE SCHEMA testschema')
df.write.mode('overwrite').saveAsTable('sales_managed')

# COMMAND ----------

# read table
from delta.tables import *
#as delta DeltaTable.forName(spark, 'sales_managed')
table = spark.read.table("sales_managed")
display(table.take(10))

# COMMAND ----------

table.schema

# COMMAND ----------

display(spark.sql('SHOW TABLES'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create external table
# MAGIC
# MAGIC External tables are tables whose data lifecycle and file layout are not managed by Unity Catalog. Use external tables to register large amounts of existing data in Unity Catalog, or if you require direct access to the data using tools outside of Azure Databricks clusters or Databricks SQL warehouses.
# MAGIC
# MAGIC When you drop an external table, Unity Catalog does not delete the underlying data. You can manage privileges on external tables and use them in queries in the same way as managed tables.

# COMMAND ----------

table.schema

# COMMAND ----------

DeltaTable.createIfNotExists().

# COMMAND ----------

CREATE TABLE sales_csv (
    
)
