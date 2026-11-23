"""Load the RDS Postgres tables into the bronze layer, one full copy per run.

    spark-submit /home/iceberg/jobs/postgres_loader.py

Reads as `etluser`, which can only SELECT. The JDBC endpoint comes from the RDS
API rather than a hard-coded host, as it would against a real AWS account.
"""

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

DB_INSTANCE = "oneshop-db"
DB_NAME = "oneshop"
USERNAME = "etluser"
PASSWORD = "etlpassword"

# Source table -> (bronze table, [(column, Iceberg type)]). Casting makes the
# Postgres -> Iceberg type mapping explicit (SERIAL arrives as INT, for example).
TABLES = {
    "users": ("bronze.users", [
        ("id", "bigint"), ("first_name", "string"), ("last_name", "string"), ("email", "string"),
        ("created_at", "timestamp"), ("updated_at", "timestamp"),
    ]),
    "items": ("bronze.items", [
        ("id", "bigint"), ("name", "string"), ("category", "string"), ("price", "decimal(7,2)"),
        ("inventory", "int"), ("created_at", "timestamp"), ("updated_at", "timestamp"),
    ]),
    "purchases": ("bronze.purchases", [
        ("id", "bigint"), ("user_id", "bigint"), ("item_id", "bigint"), ("quantity", "int"),
        ("purchase_price", "decimal(12,2)"), ("created_at", "timestamp"), ("updated_at", "timestamp"),
    ]),
}


def jdbc_url():
    endpoint = boto3.client("rds").describe_db_instances(DBInstanceIdentifier=DB_INSTANCE)["DBInstances"][0]["Endpoint"]
    return f"jdbc:postgresql://{endpoint['Address']}:{endpoint['Port']}/{DB_NAME}"


def main():
    spark = SparkSession.builder.appName("postgres-to-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    url = jdbc_url()
    print(f"Reading from {url} as {USERNAME}")

    for source, (target, columns) in TABLES.items():
        df = (
            spark.read.format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", url)
            .option("dbtable", source)
            .option("user", USERNAME)
            .option("password", PASSWORD)
            .load()
            .select(*[col(name).cast(dtype) for name, dtype in columns])
        )
        # Full refresh: replace every row. The previous load stays reachable
        # as an older snapshot.
        df.writeTo(target).overwrite(lit(True))
        print(f"{source} -> {target}: {spark.table(target).count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
