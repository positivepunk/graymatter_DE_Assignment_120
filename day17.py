# Databricks notebook source
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import sum

# COMMAND ----------


schema = StructType().add("Id", StringType(), True)\
    .add("Name", StringType(), True)\
    .add("Age",IntegerType(),True)

# COMMAND ----------

df_001=spark.read.option("header",True).option("mode","permisive").schema(schema).format("csv").load("/FileStore/gmde/db.csv")

# COMMAND ----------

df_001.display()

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://databricks@adlsgmde1710.blob.core.windows.net",
                 mount_point ="/mnt/databricks",
                 extra_configs = {"fs.azure.account.key.adlsgmde1710.blob.core.windows.net":dbutils.secrets.get(scope = "db_scope", key = "adlssecret")})

# COMMAND ----------

dbutils.fs.ls("/mnt/databricks")

# COMMAND ----------

df_002=spark.read.option("header",True).option("inferSchema",True).format("csv").load("/mnt/databricks/employee.csv")


# COMMAND ----------

df_002.display()

# COMMAND ----------

grouped_data = df_002.groupBy("Department").agg(sum("Salary").alias("dep_total"))

# COMMAND ----------

grouped_data.display()

# COMMAND ----------

df_002.write.parquet("/mnt/databricks/parquet/")

# COMMAND ----------

grouped_data.write.format("delta").saveAsTable("store_trans1")

# COMMAND ----------

df_002.write.format("delta").saveAsTable("store_trans")

# COMMAND ----------

df_002.write.format("delta").save("/mnt/databricks/storage/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_store_trans
# MAGIC USING Delta
# MAGIC OPTIONS (path "abfss://databricks@adlsgmde1710.dfs.core.windows.net/storage")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail store_trans

# COMMAND ----------

# MAGIC %sql
# MAGIC describe 
# MAGIC  store_trans

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended store_trans

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history ext_store_trans

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from ext_store_trans

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ext_store_trans
# MAGIC ZORDER BY (EmployeeID, Department);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ext_store_trans;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_store_trans Values(83,"Frank Castle","fcaste","Design",76000,"2022-07-30");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ext_store_trans
# MAGIC cluster by(EmployeeID,Department);
