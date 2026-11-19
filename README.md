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
| Spark driver UI | http://localhost:4040 |

Open `notebooks/01-oneshop-lakehouse.ipynb` and run the cells top to bottom.

## Talking to AWS from your own shell

```bash
export AWS_ACCESS_KEY_ID=000000000000
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

aws s3 ls s3://warehouse --recursive
aws glue get-databases --query 'DatabaseList[].Name'
aws glue get-table --database-name accounts --name users
```

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
  falls back to the metadata table. The notebook reads snapshots with
  PyIceberg's `tbl.inspect.snapshots()` instead.
