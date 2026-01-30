# Databricks notebook source
# MAGIC %md
# MAGIC # Simple ETL Notebook
# MAGIC This notebook reads from a source table and writes to a destination table.
# MAGIC The catalog and schema are parameterized to support different environments.

# COMMAND ----------

# Get parameters from widgets (passed from job configuration)
dbutils.widgets.text("catalog", "dev", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# Define source and destination tables
source_table = f"{catalog}.{schema}.read_table"
destination_table = f"{catalog}.{schema}.write_table"

print(f"Reading from: {source_table}")
print(f"Writing to: {destination_table}")

# COMMAND ----------

# Read data from source table
df = spark.read.table(source_table)
display(df)
print('hi')

# COMMAND ----------

# Write data to destination table
df.write.mode("overwrite").saveAsTable(destination_table)

print(f"Successfully wrote data to {destination_table}")
