import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, min, max

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# import parquet
df_clean = spark.read.parquet("s3://geotelecom-data/processed/world_towers.parquet/")

# country aggregation
geo_summary = (
    df_clean.groupBy("continent", "country")
    .agg(
        count("tower_id").alias("tower_count"),
        avg("range_m").alias("avg_range_m"),
        avg("avg_signal").alias("avg_signal"),
        min("created_ts").alias("first_created"),
        max("updated_ts").alias("last_updated")
    )
)

# network aggregation
operator_summary = (
    df_clean.groupBy("continent", "country", "network_name")
    .agg(
        count("tower_id").alias("tower_count"),
        avg("range_m").alias("avg_range_m"),
        avg("avg_signal").alias("avg_signal")
    )
)

# tower coordinates
geo_points = df_clean.select(
    "tower_id", "latitude", "longitude", "range_m", "avg_signal", "country", "continent", "network_name"
)

# save to parquet
geo_summary.write.mode("overwrite").parquet("s3://geotelecom-data/analytics/geo_summary/")
operator_summary.write.mode("overwrite").parquet("s3://geotelecom-data/analytics/operator_summary/")
geo_points.write.mode("overwrite").parquet("s3://geotelecom-data/analytics/geo_points/")