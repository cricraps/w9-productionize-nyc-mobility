# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Taxi zones
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Path to taxi zones data
taxi_zones_path = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/taxi_zones/"

# ...

if extension == ".csv":
    df = spark.read.csv(full_path, header=True, inferSchema=True)
elif extension == ".json":
    df = spark.read.json(full_path)
elif extension == ".parquet":
    df = spark.read.parquet(full_path)
else:
    df = spark.read.csv(full_path, header=True, inferSchema=True)

print("Schema:")
df.printSchema()
print(f"\nRow count: {df.count():,}")

# ...

spark.table("nyc_mobility.raw.taxi_zones").limit(20).show(truncate=False)
# Generate batch_id
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Add metadata columns
df_with_metadata = df.withColumns({
    'source_system': F.lit('NYC_TLC_Taxi_Zones'),
    'ingested_at': F.current_timestamp(),
    'batch_id': F.lit(batch_id)
})

# Save to Delta table
table_name = "nyc_mobility.raw.taxi_zones"
df_with_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"\n✓ Saved taxi zones data -> {table_name}")
print(f"  Records: {df_with_metadata.count():,}")
print(f"  Batch ID: {batch_id}")

# COMMAND ----------

# DBTITLE 1,Preview Taxi Zones Data
# Preview the ingested taxi zones data
display(spark.table("nyc_mobility.raw.taxi_zones").limit(20))
