# Databricks notebook source
dbutils.widgets.text("storage_account_key", "")

spark.conf.set(
    "fs.azure.account.key.retaildetanya2026.dfs.core.windows.net",
    dbutils.widgets.get("storage_account_key")
)

# COMMAND ----------

df_transactions = spark.read.parquet(
    "abfss://silver@retaildetanya2026.dfs.core.windows.net/transactions/"
)
df_returns = spark.read.parquet(
    "abfss://silver@retaildetanya2026.dfs.core.windows.net/returns/"
)

df_transactions.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, countDistinct, round as _round, date_format

# Add a "Revenue" column since it doesn't exist yet — this is needed by every table below
df_transactions = df_transactions.withColumn(
    "Revenue", _round(col("Quantity") * col("UnitPrice"), 2)
)

# 1. Monthly Revenue Trend
monthly_revenue = df_transactions.withColumn(
    "YearMonth", date_format(col("InvoiceDate"), "yyyy-MM")
).groupBy("YearMonth") \
 .agg(_round(_sum("Revenue"), 2).alias("TotalRevenue")) \
 .orderBy("YearMonth")

monthly_revenue.show()

# 2. Top Products by Revenue
top_products = df_transactions.groupBy("StockCode", "Description") \
 .agg(_round(_sum("Revenue"), 2).alias("TotalRevenue"),
      _sum("Quantity").alias("TotalQuantitySold")) \
 .orderBy(col("TotalRevenue").desc())

top_products.show(10)

# 3. Revenue by Country
country_revenue = df_transactions.groupBy("Country") \
 .agg(_round(_sum("Revenue"), 2).alias("TotalRevenue"),
      countDistinct("InvoiceNo").alias("NumOrders")) \
 .orderBy(col("TotalRevenue").desc())

country_revenue.show(10)

# COMMAND ----------

monthly_revenue.write.mode("overwrite").parquet(
    "abfss://gold@retaildetanya2026.dfs.core.windows.net/monthly_revenue/"
)

top_products.write.mode("overwrite").parquet(
    "abfss://gold@retaildetanya2026.dfs.core.windows.net/top_products/"
)

country_revenue.write.mode("overwrite").parquet(
    "abfss://gold@retaildetanya2026.dfs.core.windows.net/country_revenue/"
)

print("Gold layer written successfully.")

# COMMAND ----------

# JDBC connection details
jdbc_hostname = "sqlserver-retail-tanya-2026.database.windows.net"
jdbc_port = 1433
jdbc_database = "sqldb-retail-gold"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};encrypt=true;trustServerCertificate=false;loginTimeout=30"


dbutils.widgets.text("sql_user", "")
dbutils.widgets.text("sql_password", "")

connection_properties = {
    "user": dbutils.widgets.get("sql_user"),
    "password": dbutils.widgets.get("sql_password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
# Write each Gold table to SQL
monthly_revenue.write.jdbc(
    url=jdbc_url, table="monthly_revenue", mode="overwrite", properties=connection_properties
)

top_products.write.jdbc(
    url=jdbc_url, table="top_products", mode="overwrite", properties=connection_properties
)

country_revenue.write.jdbc(
    url=jdbc_url, table="country_revenue", mode="overwrite", properties=connection_properties
)

print("Gold tables loaded into Azure SQL Database successfully.")