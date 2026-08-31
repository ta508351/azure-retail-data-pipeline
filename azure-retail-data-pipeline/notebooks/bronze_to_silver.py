# Databricks notebook source
dbutils.widgets.text("storage_account_key", "")

spark.conf.set(
    "fs.azure.account.key.retaildetanya2026.dfs.core.windows.net",
    dbutils.widgets.get("storage_account_key")
)

# COMMAND ----------

df = spark.read.csv( "abfss://bronze@retaildetanya2026.dfs.core.windows.net/retail/2026/08/29/online_retail.csv", header=True, inferSchema=True ) 
df.show(5) 
df.printSchema() 

# COMMAND ----------

print("Total Rows", df.count())

# COMMAND ----------

from pyspark.sql.functions import col, count, when
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

df.filter(col("InvoiceNo").startswith("C")).show(5)

# COMMAND ----------

df.filter(col("Quantity") < 0).show(5)

# Check for duplicate rows
print("Duplicate rows:", df.count() - df.dropDuplicates().count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper

# Trim whitespace from string columns
df_clean = df.withColumn("Description", trim(col("Description"))) \
             .withColumn("StockCode", trim(col("StockCode"))) \
             .withColumn("Country", trim(col("Country")))

# Drop exact duplicate rows
df_clean = df_clean.dropDuplicates()

# Split into transactions (valid sales) and returns (cancellations)
df_returns = df_clean.filter(col("InvoiceNo").startswith("C"))
df_transactions = df_clean.filter(~col("InvoiceNo").startswith("C"))

# Drop rows with null Description (small enough to discard safely)
df_transactions = df_transactions.filter(col("Description").isNotNull())

print("Transactions:", df_transactions.count())
print("Returns:", df_returns.count())

# Write both to Silver as Parquet
df_transactions.write.mode("overwrite").parquet(
    "abfss://silver@retaildetanya2026.dfs.core.windows.net/transactions/"
)
df_returns.write.mode("overwrite").parquet(
    "abfss://silver@retaildetanya2026.dfs.core.windows.net/returns/"
)

print("Silver layer written successfully.")

# COMMAND ----------

