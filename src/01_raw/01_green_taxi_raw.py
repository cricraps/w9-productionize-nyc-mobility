# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Ingest Green Taxi Data
from pyspark.sql import functions as F
from datetime import datetime
from delta.tables import DeltaTable

volume_path = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/green_taxi/"


# --- Job parameter / widget: comma-separated list of YYYY-MM months ---
dbutils.widgets.text("months", "2026-03,2026-04,2026-05", "Months to load (YYYY-MM, comma-separated)")
months_param = dbutils.widgets.get("months").strip()

assert months_param, (
    "FAIL: 'months' parameter is empty. This notebook does not fall back to a default — "
    "pass an explicit months value via the widget or job parameter."
)

months = []
for m in months_param.split(","):
    m = m.strip()
    year, month = m.split("-")
    filename = f"green_tripdata_{year}-{month}.parquet"
    table_name = f"green_{month}_{year}"
    months.append((filename, table_name, m))

batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

NATURAL_KEY_COLS = [
    "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_distance", "fare_amount"
]

def file_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def load_green_taxi_month(filename: str, table_name: str, batch_id: str, month_label: str):
    file_path = f"{volume_path}{filename}"

    # GUARD — catch a missing source file before attempting to read it
    if not file_exists(file_path):
        print(f"SKIPPED {month_label}: file not found at {file_path}")
        return None  # recover — don't crash the whole job over one missing month

    df = spark.read.parquet(file_path)

    source_row_count = df.count()
    assert source_row_count > 0, f"FAIL: source file {filename} contains 0 rows — aborting load"

    df_with_metadata = df.withColumns({
        "source_system": F.lit("NYC_TLC_Green_Taxi"),
        "ingested_at": F.current_timestamp(),
        "batch_id": F.lit(batch_id)
    })

    full_table = f"nyc_mobility.raw.{table_name}"

    if not spark.catalog.tableExists(full_table):
        df_with_metadata.write.format("delta").saveAsTable(full_table)
        row_count = source_row_count
        print(f"Created {full_table} ({row_count} rows) [batch_id: {batch_id}]")
    else:
        delta_tbl = DeltaTable.forName(spark, full_table)
        merge_condition = " AND ".join([f"t.{c} <=> s.{c}" for c in NATURAL_KEY_COLS])
        (
            delta_tbl.alias("t")
            .merge(df_with_metadata.alias("s"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

        op_metrics = spark.sql(f"DESCRIBE HISTORY {full_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        rows_inserted = int(op_metrics.get("numTargetRowsInserted", 0))
        row_count = spark.table(full_table).count()

        print(f"Merged {filename} -> {full_table}: {rows_inserted} new rows inserted, table now {row_count} rows [batch_id: {batch_id}]")

    assert row_count > 0, f"FAIL: {full_table} has 0 rows after load — pipeline did not land data"
    return row_count

# COMMAND ----------

# --- run for the months that were passed in the widget ---
results = {}
for filename, table_name, month_label in months:
    results[month_label] = load_green_taxi_month(filename, table_name, batch_id, month_label)

print("\nLoad summary:")
for month_label, count in results.items():
    status = "OK" if count else "SKIPPED (missing file)"
    print(f"  {month_label}: {status} {count if count else ''}")

# COMMAND ----------

# Idempotency proof
target = "nyc_mobility.raw.green_05_2026"
count_before = spark.table(target).count()

rerun_batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
load_green_taxi_month("green_tripdata_2026-05.parquet", "green_05_2026", rerun_batch_id, "2026-05")

count_after = spark.table(target).count()
assert count_before == count_after, f"NOT idempotent: {count_before} -> {count_after}"
print(f"Idempotency confirmed: {count_before} rows before and after re-running May.")
