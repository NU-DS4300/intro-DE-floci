"""Clean, validate, and enrich the bronze tables into the silver layer.

    spark-submit /home/iceberg/jobs/bronze_to_silver.py

silver.users                users plus valid_email and full_name
silver.items                negative prices clamped to 0, categories upper-cased
silver.purchases_enriched   purchases joined with users and items, plus totals and date parts
silver.pageviews_by_items   product page views with the item id parsed from the URL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, hour, lit, regexp_extract, to_date, upper, when

# Basic shape check: something@domain.tld
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
# Product pages look like /products/42; everything else (/, /cart, ...) is dropped.
ITEM_PAGE_REGEX = r"^/([^/]+)/(\d+)$"


def write(df, table):
    # Replace only the partitions this run produced; Iceberg keeps the old
    # files reachable through earlier snapshots.
    df.writeTo(table).overwritePartitions()
    print(f"{table}: {df.sparkSession.table(table).count()} rows")


def main():
    spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    users = spark.table("bronze.users")
    items = spark.table("bronze.items")
    purchases = spark.table("bronze.purchases")
    pageviews = spark.table("bronze.pageviews")

    silver_users = users.select(
        "id", "first_name", "last_name", "email", "created_at", "updated_at",
        # rlike on a NULL email is NULL; a missing email is not a valid one.
        coalesce(col("email").rlike(EMAIL_REGEX), lit(False)).alias("valid_email"),
        concat_ws(" ", "first_name", "last_name").alias("full_name"),
    )

    silver_items = items.select(
        "id",
        "name",
        upper("category").alias("category"),
        # The CASE widens the type; cast back to the table's DECIMAL(7,2).
        when(col("price") < 0, lit(0)).otherwise(col("price")).cast("decimal(7,2)").alias("price"),
        "inventory",
        "created_at",
        "updated_at",
    )

    p, u, i = purchases.alias("p"), users.alias("u"), items.alias("i")
    silver_purchases = (
        p.join(u, col("p.user_id") == col("u.id"), "left")
        .join(i, col("p.item_id") == col("i.id"), "left")
        .select(
            col("p.id"),
            col("p.user_id"),
            col("p.item_id"),
            col("p.quantity"),
            col("p.purchase_price"),
            (col("p.quantity") * col("p.purchase_price")).cast("decimal(14,2)").alias("total_price"),
            col("u.email").alias("user_email"),
            col("i.name").alias("item_name"),
            upper(col("i.category")).alias("item_category"),
            to_date(col("p.created_at")).alias("purchase_date"),
            hour(col("p.created_at")).alias("purchase_hour"),
            col("p.created_at"),
            col("p.updated_at"),
        )
    )

    item_views = (
        pageviews.withColumn("page", regexp_extract("url", ITEM_PAGE_REGEX, 1))
        .withColumn("item_id", regexp_extract("url", ITEM_PAGE_REGEX, 2))
        .filter(col("item_id") != "")
        .withColumn("item_id", col("item_id").cast("bigint"))
        .alias("v")
    )
    silver_pageviews = item_views.join(i, col("v.item_id") == col("i.id"), "left").select(
        col("v.user_id"),
        col("v.item_id"),
        col("v.page"),
        col("i.name").alias("item_name"),
        upper(col("i.category")).alias("item_category"),
        col("v.channel"),
        col("v.received_at"),
    )

    write(silver_users, "silver.users")
    write(silver_items, "silver.items")
    write(silver_purchases, "silver.purchases_enriched")
    write(silver_pageviews, "silver.pageviews_by_items")

    spark.stop()


if __name__ == "__main__":
    main()
