"""Load the page-view JSON events from S3 into bronze.pageviews, one full copy per run.

    spark-submit /home/iceberg/jobs/pageviews_loader.py

The events are plain JSON Lines files, not an Iceberg table, so Spark reads
them through Hadoop's S3A connector (s3a://). spark-defaults.conf points S3A
at Floci and takes the credentials from the AWS_* environment variables.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

SOURCE = "s3a://oneshop-pageviews/"
TARGET = "bronze.pageviews"
# Declaring the schema skips a full pass over the files to infer it, and parses
# the ISO-8601 received_at strings straight into timestamps.
SCHEMA = "user_id BIGINT, url STRING, channel STRING, received_at TIMESTAMP"


def main():
    spark = SparkSession.builder.appName("pageviews-to-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Objects live under yyyy/mm/dd/hh/; read every file below the bucket root.
    pageviews = spark.read.schema(SCHEMA).option("recursiveFileLookup", "true").json(SOURCE)
    pageviews.writeTo(TARGET).overwrite(lit(True))
    print(f"{SOURCE} -> {TARGET}: {spark.table(TARGET).count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
