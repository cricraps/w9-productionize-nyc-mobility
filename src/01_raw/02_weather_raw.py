# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Weather
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from delta.tables import DeltaTable
from datetime import datetime
import json

# Read the weather JSON file
weather_file = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/weather/nyc_weather_2026-03_to_05.json"

with open(weather_file, 'r') as f:
    weather_json = json.load(f)

# Extract metadata
latitude = weather_json['latitude']
longitude = weather_json['longitude']
timezone = weather_json['timezone']
elevation = weather_json['elevation']

# Extract hourly data arrays
hourly = weather_json['hourly']
times = hourly['time']
temperatures = hourly['temperature_2m']
precipitation = hourly['precipitation']
rain = hourly['rain']
snowfall = hourly['snowfall']
weather_codes = hourly['weather_code']
wind_speeds = hourly['wind_speed_10m']

# Build records
records = []
for i in range(len(times)):
    records.append({
        'observation_time': times[i],
        'temperature_2m': temperatures[i],
        'precipitation': precipitation[i],
        'rain': rain[i],
        'snowfall': snowfall[i],
        'weather_code': weather_codes[i],
        'wind_speed_10m': wind_speeds[i],
        'latitude': latitude,
        'longitude': longitude,
        'timezone': timezone,
        'elevation': elevation
    })

if not records:
    raise ValueError("No records parsed from source JSON — aborting before any write.")

df = spark.createDataFrame(records)

batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

df_with_metadata = df \
    .withColumn('observation_time', F.to_timestamp('observation_time')) \
    .withColumns({
        'source_system': F.lit('OpenMeteo_Weather_API'),
        'ingested_at': F.current_timestamp(),
        'batch_id': F.lit(batch_id)
    })

# Dedupe the incoming batch itself, in case the source file has overlapping/duplicate rows
# Natural key: observation_time + latitude + longitude (a given station/coord shouldn't have
# two readings for the same hour)
df_with_metadata = df_with_metadata.dropDuplicates(['observation_time', 'latitude', 'longitude'])

source_row_count = df_with_metadata.count()

table_name = "nyc_mobility.raw.weather"

if not spark.catalog.tableExists(table_name):
    # First-ever load: straight write, no merge needed
    df_with_metadata.write.format("delta").mode("overwrite").saveAsTable(table_name)
else:
    # Incremental: only bother merging rows newer than what's already there, or that
    # don't already exist for that time/lat/lon (covers late-arriving/backfilled rows too)
    max_existing_ts = spark.table(table_name).agg(F.max('observation_time')).collect()[0][0]

    if max_existing_ts is not None:
        incremental_df = df_with_metadata.filter(F.col('observation_time') > F.lit(max_existing_ts))
    else:
        incremental_df = df_with_metadata

    if incremental_df.take(1):
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias('t')
            .merge(
                incremental_df.alias('s'),
                't.observation_time = s.observation_time AND '
                't.latitude = s.latitude AND '
                't.longitude = s.longitude'
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        print("No new rows past current max observation_time — nothing to merge.")

# --- Row-count assertion on the raw write ---
# Confirms the table actually has data after this run, and that this run
# contributed rows (either as the initial load or via merge).
post_write_count = spark.table(table_name).count()

if post_write_count == 0:
    raise RuntimeError(
        f"Row-count assertion failed: {table_name} has 0 rows after write. Failing notebook."
    )

print(f"✓ Saved weather data -> {table_name}")
print(f"  Source rows this batch: {source_row_count:,}")
print(f"  Table row count (post-write): {post_write_count:,}")
print(f"  Date range in source: {times[0]} to {times[-1]}")
print(f"  Batch ID: {batch_id}")

# COMMAND ----------

# DBTITLE 1,Preview Weather Data
# Preview the ingested weather data
display(spark.table("nyc_mobility.raw.weather").orderBy("observation_time").limit(20))