#!/bin/sh
# Runs in the spark-iceberg container before Jupyter starts (see
# docker-compose.yaml); the Floci image has no AWS CLI. This replaces the `mc`
# container from the original chapter: same job (make the bucket the lakehouse
# writes into), done with the AWS CLI against emulated S3 and Glue.
# Idempotent: safe to rerun on every container start.
set -eu

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"

ensure_bucket() {
  if aws --endpoint-url "$ENDPOINT" s3api head-bucket --bucket "$1" >/dev/null 2>&1; then
    echo "[bootstrap] s3://$1 exists"
  else
    echo "[bootstrap] creating s3://$1"
    aws --endpoint-url "$ENDPOINT" s3 mb "s3://$1"
  fi
}

ensure_bucket warehouse

if aws --endpoint-url "$ENDPOINT" glue get-database --name accounts >/dev/null 2>&1; then
  echo "[bootstrap] Glue database 'accounts' exists"
else
  echo "[bootstrap] creating Glue database 'accounts'"
  aws --endpoint-url "$ENDPOINT" glue create-database \
    --database-input '{"Name":"accounts","Description":"OneShop user accounts","LocationUri":"s3://warehouse/accounts"}'
fi

ensure_bucket athena-results

echo "[bootstrap] done"
