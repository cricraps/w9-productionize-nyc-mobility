# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Ingest Taxi Zones
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

# SAFE FILE DISCOVERY
# Only pick up known data extensions, and require exactly one match
# Prevents silently ingesting the wrong file (backup copy, .crc, stray old version)
files = [f for f in os.listdir(path)
         if os.path.isfile(os.path.join(path, f)) and os.path.splitext(f)[1].lower() in ALLOWED_EXT]

if len(files) != 1:
    raise ValueError(f"Expected exactly 1 data file in {path}, found {len(files)}: {files}")

data_file = files[0]
ext = os.path.splitext(data_file)[1].lower()
full_path = os.path.join(path, data_file)

# READ BASED ON FILE EXTENSION
if ext == ".csv":
    df = spark.read.csv(full_path, header=True, inferSchema=True)
elif ext == ".json":
    df = spark.read.json(full_path)
else:
    df = spark.read.parquet(full_path)

# GUARD — catch an empty source file before it gets written downstream
row_count = df.count()
if row_count == 0:
    raise ValueError(f"{data_file} produced 0 rows, aborting")

# GUARD — catch a schema drift (renamed/missing column) before it silently breaks clean layer joins
EXPECTED_COLS = {"LocationID", "Borough", "Zone", "service_zone"}
missing = EXPECTED_COLS - set(df.columns)
if missing:
    raise ValueError(f"Missing expected columns: {missing}. Found: {df.columns}")

# DEDUPLICATION
# LocationID is a real primary key here, so a plain distinct-count check is enough
distinct_count = df.select("LocationID").distinct().count()
if distinct_count != row_count:
    print(f"Found {row_count - distinct_count} duplicate LocationID rows, deduping")
    df = df.dropDuplicates(["LocationID"])
    row_count = df.count()

# ADD TRACKING METADATA
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
df_out = df.withColumns({
    "source_system": F.lit("NYC_TLC_Taxi_Zones"),
    "ingested_at": F.current_timestamp(),
    "batch_id": F.lit(batch_id)
}).cache()
final_count = df_out.count()

# CAPTURE PRE-WRITE STATE FOR THE IDEMPOTENCY CHECK BELOW 
table = "nyc_mobility.raw.taxi_zones"
prev_count = spark.table(table).count() if spark.catalog.tableExists(table) else None

# Zones is a small, full-refresh reference table (one file = the complete current state) (unlike green taxi's incremental monthly MERGE) 
# Overwrite is correct and simplest here: same file in, same table out, regardless of how many times this runs.
df_out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

print(f"Saved {table}: {final_count:,} rows, batch {batch_id}")

# IDEMPOTENCY CHECK
# Since there's no incremental insert step to test (unlike green taxi's MERGE), idempotency here means: did this run produce the same row count as before it ran
if prev_count is not None:
    status

# COMMAND ----------

# DBTITLE 1,Preview Taxi Zones Data
# Preview the ingested taxi zones data
spark.table("nyc_mobility.raw.taxi_zones").limit(20).show(truncate=False)
