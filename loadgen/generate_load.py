"""Create OneShop's source systems on Floci and fill them with mock data.

- Amazon RDS for PostgreSQL instance `oneshop-db`, database `oneshop`: the
  users, items, and purchases tables, plus a read-only `etluser` login.
- S3 bucket `oneshop-pageviews`: website page-view events as JSON Lines, one
  object per hour under yyyy/mm/dd/hh/ (the layout Kinesis Data Firehose uses).

The data carries a few deliberate defects for the silver layer to handle:
missing and malformed emails, negative item prices, lowercase categories, and
page views of pages that are not items.

Safe to rerun: an existing instance, populated tables, or a non-empty bucket
is left as it is.
"""

import json
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import psycopg
from botocore.exceptions import ClientError
from faker import Faker

DB_INSTANCE = "oneshop-db"
DB_NAME = "oneshop"
MASTER_USER = "postgresuser"
MASTER_PASSWORD = "postgrespw"
BUCKET = "oneshop-pageviews"

N_USERS = 10_000
N_ITEMS = 1_000
N_PURCHASES = 20_000
N_PAGEVIEWS = 20_000
USER_HISTORY_DAYS = 90  # users signed up over the last 90 days
ACTIVITY_DAYS = 7  # purchases and page views cover the last 7 days

CATEGORIES = ["electronics", "books", "home", "garden", "toys", "sports", "beauty", "grocery", "clothing", "office"]
ADJECTIVES = ["Classic", "Smart", "Compact", "Deluxe", "Eco", "Portable", "Premium", "Rustic", "Ultra", "Vintage"]
NOUNS = ["Lamp", "Backpack", "Blender", "Notebook", "Speaker", "Kettle", "Chair", "Puzzle", "Jacket", "Planter",
         "Headphones", "Mug", "Racket", "Serum", "Desk", "Novel", "Drone", "Candle", "Sneakers", "Toaster"]
CHANNELS = ["web", "mobile_app", "email", "social"]
CHANNEL_WEIGHTS = [50, 30, 10, 10]
NON_ITEM_PAGES = ["/", "/cart", "/checkout", "/search", "/account"]

rng = random.Random(4300)
Faker.seed(4300)
fake = Faker()
# Postgres TIMESTAMP columns hold naive UTC.
NOW = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def log(message):
    print(f"[loadgen] {message}")


def ensure_rds_instance():
    """Create the RDS instance if needed and return its endpoint once it is available."""
    rds = boto3.client("rds")
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE,
            Engine="postgres",
            DBInstanceClass="db.t3.micro",
            AllocatedStorage=20,
            DBName=DB_NAME,
            MasterUsername=MASTER_USER,
            MasterUserPassword=MASTER_PASSWORD,
        )
        log(f"creating RDS instance {DB_INSTANCE}")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("DBInstanceAlreadyExists", "DBInstanceAlreadyExistsFault"):
            raise
        log(f"RDS instance {DB_INSTANCE} exists")

    while True:
        db = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE)["DBInstances"][0]
        if db["DBInstanceStatus"] == "available":
            endpoint = db["Endpoint"]
            log(f"RDS instance available at {endpoint['Address']}:{endpoint['Port']}")
            return endpoint
        time.sleep(2)


def connect(endpoint):
    """Connect as the master user, waiting for Postgres inside the instance to accept connections."""
    deadline = time.monotonic() + 120
    while True:
        try:
            return psycopg.connect(
                host=endpoint["Address"],
                port=endpoint["Port"],
                dbname=DB_NAME,
                user=MASTER_USER,
                password=MASTER_PASSWORD,
                autocommit=True,
            )
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def random_time(days, end=NOW):
    return end - timedelta(seconds=rng.uniform(0, days * 86_400))


def user_rows():
    for _ in range(N_USERS):
        first, last = fake.first_name(), fake.last_name()
        roll = rng.random()
        if roll < 0.03:
            email = None
        elif roll < 0.06:
            # Malformed: no domain, no @, or no top-level domain.
            local = f"{first}.{last}".lower()
            email = rng.choice([f"{local}@", f"{local}.example.com", f"{local}@example"])
        else:
            email = fake.email()
        created = random_time(USER_HISTORY_DAYS)
        yield first, last, email, created, created + timedelta(days=rng.uniform(0, 3))


def item_rows():
    for _ in range(N_ITEMS):
        price = round(rng.uniform(2, 500), 2)
        if rng.random() < 0.01:
            price = -price  # data-entry error the silver layer clamps to 0
        created = random_time(365)
        yield (
            f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS)}",
            rng.choice(CATEGORIES),
            price,
            rng.randint(0, 500),
            created,
            created + timedelta(days=rng.uniform(0, 30)),
        )


def purchase_rows(item_prices):
    for _ in range(N_PURCHASES):
        item_id = rng.randint(1, N_ITEMS)
        created = random_time(ACTIVITY_DAYS)
        yield (
            rng.randint(1, N_USERS),
            item_id,
            rng.randint(1, 5),
            abs(item_prices[item_id]),  # unit price at the time of purchase
            created,
            created + timedelta(minutes=rng.uniform(0, 120)),
        )


def copy_rows(cur, table, columns, rows):
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def populate_postgres(conn):
    with conn.cursor() as cur:
        cur.execute(Path(__file__).with_name("bootstrap.sql").read_text())
        cur.execute("SELECT count(*) FROM users")
        if cur.fetchone()[0] > 0:
            log("Postgres tables already populated")
            return

        items = list(item_rows())
        copy_rows(cur, "users", ["first_name", "last_name", "email", "created_at", "updated_at"], user_rows())
        copy_rows(cur, "items", ["name", "category", "price", "inventory", "created_at", "updated_at"], items)
        # SERIAL ids start at 1 in insert order.
        item_prices = {i: row[2] for i, row in enumerate(items, start=1)}
        copy_rows(
            cur,
            "purchases",
            ["user_id", "item_id", "quantity", "purchase_price", "created_at", "updated_at"],
            purchase_rows(item_prices),
        )
        log(f"loaded {N_USERS} users, {N_ITEMS} items, {N_PURCHASES} purchases into Postgres")


def populate_pageviews():
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        log(f"created s3://{BUCKET}")
    if s3.list_objects_v2(Bucket=BUCKET, MaxKeys=1)["KeyCount"] > 0:
        log(f"s3://{BUCKET} already has events")
        return

    rng.seed(4301)  # same events whether or not Postgres was populated in this run
    by_hour = defaultdict(list)
    for _ in range(N_PAGEVIEWS):
        received = random_time(ACTIVITY_DAYS)
        url = f"/products/{rng.randint(1, N_ITEMS)}" if rng.random() < 0.85 else rng.choice(NON_ITEM_PAGES)
        event = {
            "user_id": rng.randint(1, N_USERS),
            "url": url,
            "channel": rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0],
            "received_at": received.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        by_hour[received.replace(minute=0, second=0, microsecond=0)].append(event)

    for hour, events in sorted(by_hour.items()):
        events.sort(key=lambda e: e["received_at"])
        s3.put_object(
            Bucket=BUCKET,
            Key=f"{hour:%Y/%m/%d/%H}/pageviews-{hour:%Y%m%d%H}.json",
            Body="\n".join(json.dumps(e) for e in events).encode(),
        )
    log(f"wrote {N_PAGEVIEWS} page views to s3://{BUCKET} as {len(by_hour)} hourly objects")


def main():
    endpoint = ensure_rds_instance()
    with connect(endpoint) as conn:
        populate_postgres(conn)
    populate_pageviews()
    log("done")


if __name__ == "__main__":
    main()
