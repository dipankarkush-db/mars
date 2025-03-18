# Databricks notebook source
# MAGIC %sql
# MAGIC revoke read files on external location `dkushari-ext-loc-1` from `dipankar.kushari@databricks.com`;

# COMMAND ----------

# DBTITLE 1,External Location owner is a SP
# MAGIC %sql
# MAGIC desc EXTERNAL LOCATION `dkushari-ext-loc-1`;

# COMMAND ----------

# DBTITLE 1,No Grants available
# MAGIC %sql
# MAGIC show grants on external location `dkushari-ext-loc-1`;

# COMMAND ----------

path = "abfss://dkushari@dkusharistorageaccount.dfs.core.windows.net/marsdemo/csv/volumedemo/sample/sample_data.csv"

# COMMAND ----------

# DBTITLE 1,Uses fallback Mode since UC permission is missing
spark.read.format('csv').option("header","true").load(path).display()

# COMMAND ----------

# DBTITLE 1,Read files granted
# MAGIC %sql
# MAGIC grant read files on external location `dkushari-ext-loc-1` to `dipankar.kushari@databricks.com`;

# COMMAND ----------

# DBTITLE 1,Showing read files
# MAGIC %sql
# MAGIC show grants on external location `dkushari-ext-loc-1`;

# COMMAND ----------

spark.read.format('csv').option("header","true").load(path).display()