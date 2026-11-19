# Chapter 2 — The OneShop Data Lakehouse, on AWS service names

This is the chapter's minimal lakehouse rebuilt so that every storage and
metadata component is a real **AWS service API**, served locally by
[Floci](https://floci.io) — a free, open-source AWS emulator. Students learn
S3, Glue, and Athena by name, and the code they write is the code they would
write against a live AWS account.

## What changed from the book

| Book's component | Here | Notes |
| --- | --- | --- |
| `minio` container | **Amazon S3** | Same bucket, real S3 API |
| `mc` client container | **AWS CLI** | `aws s3 mb` replaces the `until … sleep 1` loop |
| `rest` (`iceberg-rest-fixture`) | **AWS Glue Data Catalog** | Glue's Iceberg REST endpoint at `/iceberg` |
| MinIO console on `:9001` | **Floci console** on `:4500` | Browse buckets and catalog entries |
| — | **Amazon Athena** | New: serverless SQL over the same table |
| `spark-iceberg` container | unchanged | Floci emulates control planes, not Spark |

Four containers become two. Spark stays because an emulator can't replace a
query engine; it is configured exactly as it would be against real Glue.

## Running it

```bash
docker compose build
docker compose up -d
```

First build downloads Spark and the Iceberg jars, so expect a few minutes.
Wait for Floci to report healthy (`docker compose ps`), then:

| Component | URL |
| --- | --- |
| Jupyter Lab | http://localhost:8888 |
| Floci resource console | http://localhost:4566/_floci/ui (first visit starts it, then redirects to `:4500`) |
| AWS service endpoint | http://localhost:4566 |
| Spark driver UI | http://localhost:4040 (while a Spark session is running) |

In Jupyter Lab, notebooks you create are saved to `notebooks/` on your laptop.
A `%%sql` cell runs one Spark SQL statement against the Glue catalog
(`spark/ipython-startup.py` defines the magic). The same SQL also works in
`docker exec -it spark-iceberg spark-sql`.

## Creating the first table

The bootstrap script (`init/01-bootstrap-lake.sh`) has already created the
`warehouse` bucket and the Glue database `accounts`. Load the sample OneShop
users (`data/users.csv`) into a partitioned Iceberg table, one statement per
`%%sql` cell:

```sql
CREATE TABLE IF NOT EXISTS accounts.users (
    id INT, first_name STRING, last_name STRING, email STRING
) USING iceberg
PARTITIONED BY (truncate(1, last_name))
```

```sql
CREATE OR REPLACE TEMPORARY VIEW users_csv USING csv
OPTIONS (path '/home/iceberg/data/users.csv', header 'true', inferSchema 'true')
```

```sql
INSERT INTO accounts.users SELECT * FROM users_csv
```

`truncate(1, last_name)` is Iceberg hidden partitioning: one
`last_name_trunc=<letter>` prefix per first letter, no extra column. Spark
wrote the files, but Glue now holds the table; every command below sees it.

## Talking to AWS from your own shell

```bash
set -a; . ./.env.example; set +a   # or copy it to .env and source that

aws s3 ls s3://warehouse --recursive
aws glue get-databases --query 'DatabaseList[].Name'
aws glue get-table --database-name accounts --name users
```

Athena queries the same table through the Glue catalog; results land in the
`athena-results` bucket the bootstrap script created:

```bash
QID=$(aws athena start-query-execution \
  --query-string 'SELECT last_name, email FROM users ORDER BY last_name' \
  --query-execution-context Database=accounts \
  --result-configuration OutputLocation=s3://athena-results/ \
  --query QueryExecutionId --output text)
# Athena is asynchronous: wait for the query to leave QUEUED/RUNNING.
while aws athena get-query-execution --query-execution-id "$QID" \
    --query QueryExecution.Status.State --output text | grep -qE 'QUEUED|RUNNING'; do
  sleep 1
done
aws athena get-query-results --query-execution-id "$QID"
```

Inside the container (`docker exec -it spark-iceberg bash` or a JupyterLab
terminal) the credentials and endpoint are already set.

The PyIceberg CLI works as it does in the book:

```bash
docker exec -it spark-iceberg pyiceberg --catalog glue list
docker exec -it spark-iceberg pyiceberg --catalog glue describe --entity=table accounts.users
```

## Two things that trip people up

**1. The access key is twelve digits.** Floci treats a 12-digit
`AWS_ACCESS_KEY_ID` as the AWS account id. That matters because on Glue's
Iceberg REST endpoint the catalog `warehouse` is *the account id*, not an
`s3://` path:

```
spark.sql.catalog.glue.warehouse   000000000000
```

Set a key like `test` instead and the account resolves to `000000000000`
anyway, but the mismatch with `warehouse` is a confusing failure. Keep them
equal.

**2. Requests must be SigV4-signed.** The book's `iceberg-rest-fixture`
accepted unsigned requests; Glue does not. Hence
`rest.auth.type=sigv4` + `rest.signing-name=glue` in
`spark-defaults.conf`, the `iceberg-aws-bundle` jar in the Dockerfile, and
PyIceberg's `rest-sigv4` extra. PyIceberg is bumped from the book's `0.7.1`
to `0.12.0`, which is the version range where this is properly supported.

## State

`FLOCI_STORAGE_MODE=hybrid` keeps the catalog and buckets across
`docker compose restart`. For a clean slate each run, set it to `memory`.
A full reset:

```bash
docker compose down -v
```

## Known limits

- Athena executes via a DuckDB sidecar, not Trino, so exotic Trino SQL may
  differ. Ordinary `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE` on Iceberg work.
- `CREATE TABLE AS` with `table_type='ICEBERG'` and `OPTIMIZE` are not yet
  supported in Athena emulation.
- Floci needs the host Docker socket, because Athena and the console run as
  sidecar containers.
- Floci is pinned to a dated nightly (`nightly-11152026`): Glue's Iceberg REST
  endpoint is newer than the latest release, 2.5.0. Move to the first release
  that includes it once one ships.
- Spark's metadata-table syntax (`glue.accounts.users.snapshots`) fails against
  Floci: Iceberg first tries `accounts.users` as a namespace, and Floci answers
  that two-level namespace with an error rather than "not found", so Spark never
  falls back to the metadata table. Read snapshots with PyIceberg instead:
  `load_catalog('glue').load_table('accounts.users').inspect.snapshots()`.
