from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Path to taxi zones data
taxi_zones_path = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/taxi_zones/"

# Select the first supported input file
supported_extensions = {".csv", ".json", ".parquet"}

input_files = [
    filename
    for filename in os.listdir(taxi_zones_path)
    if os.path.isfile(os.path.join(taxi_zones_path, filename))
    and os.path.splitext(filename)[1].lower() in supported_extensions
]

if not input_files:
    raise FileNotFoundError(
        f"No CSV, JSON, or Parquet files found in {taxi_zones_path}"
    )

filename = input_files[0]
full_path = os.path.join(taxi_zones_path, filename)
extension = os.path.splitext(filename)[1].lower()

if extension == ".csv":
    df = spark.read.csv(full_path, header=True, inferSchema=True)
elif extension == ".json":
    df = spark.read.json(full_path)
elif extension == ".parquet":
    df = spark.read.parquet(full_path)
else:
    raise ValueError(f"Unsupported file extension: {extension}")

print("Schema:")
df.printSchema()
print(f"\nRow count: {df.count():,}")

spark.table("nyc_mobility.raw.taxi_zones").limit(20).show(
    truncate=False
)
