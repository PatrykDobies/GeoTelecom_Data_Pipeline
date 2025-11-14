import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, from_unixtime
from pyspark.sql.types import IntegerType, DoubleType, StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# World Cell Towers ETL - Combined Data

# List of continents and files
continents = [
    ("Africa", "s3://geotelecom-data/raw/Africa towers.csv"),
    ("Asia", "s3://geotelecom-data/raw/Asia towers.csv"),
    ("Europe", "s3://geotelecom-data/raw/Europe towers.csv"),
    ("North America", "s3://geotelecom-data/raw/North America towers.csv"),
    ("South America", "s3://geotelecom-data/raw/South America towers.csv"),
    ("Oceania", "s3://geotelecom-data/raw/Oceania towers.csv")
]

dfs = []

for name, path in continents:
    print(f"Reading {path} ...")

    # Loading data
    df = spark.read.option("header", True).csv(path)

    # Fixing missing header
    old_cols = df.columns
    if old_cols[0] == "" or old_cols[0].startswith("_c"):
        old_cols[0] = "tower_id"
        df = df.toDF(*old_cols)

    # Unification of headers
    rename_map = {
        "LON": "longitude",
        "LAT": "latitude",
        "RANGE": "range_m",
        "SAM": "sample_count",
        "changeable": "is_changeable",
        "created": "created_ts",
        "updated": "updated_ts",
        "averageSignal": "avg_signal",
        "Country": "country",
        "Network": "network_name",
        "Continent": "continent"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    dfs.append(df)

# Merging continents

print("Merging dataframes...")
df_all = dfs[0]
for d in dfs[1:]:
    df_all = df_all.unionByName(d, allowMissingColumns=True)

# Cleaning data

print("Cleaning data...")
df_clean = (
    df_all
    .dropDuplicates(["tower_id"])
    .dropna(subset=["latitude", "longitude"])
)

# types

df_clean = df_clean.withColumn("tower_id", df_clean["tower_id"].cast(IntegerType())) \
    .withColumn("mcc", df_clean["mcc"].cast(IntegerType())) \
    .withColumn("mnc", df_clean["mnc"].cast(IntegerType())) \
    .withColumn("tac", df_clean["tac"].cast(IntegerType())) \
    .withColumn("cid", df_clean["cid"].cast(IntegerType())) \
    .withColumn("longitude", df_clean["longitude"].cast(DoubleType())) \
    .withColumn("latitude", df_clean["latitude"].cast(DoubleType())) \
    .withColumn("range_m", df_clean["range_m"].cast(DoubleType())) \
    .withColumn("sample_count", df_clean["sample_count"].cast(DoubleType())) \
    .withColumn("created_ts", from_unixtime(col("created_ts")).cast("timestamp")) \
    .withColumn("updated_ts", from_unixtime(col("updated_ts")).cast("timestamp")) \
    .withColumn("avg_signal", df_clean["avg_signal"].cast(DoubleType()))

# Save to Parquet (S3)

output_path = "s3://geotelecom-data/processed/world_towers.parquet"
print(f"Saving cleaned data to {output_path}")
df_clean.write.mode("overwrite").parquet(output_path)

print("ETL completed successfully!")

job.commit()