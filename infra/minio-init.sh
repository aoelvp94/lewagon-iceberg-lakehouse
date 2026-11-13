#!/bin/sh
# Create the two buckets: landing (source drops) and warehouse (Iceberg files).
set -e
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do sleep 1; done
mc mb --ignore-existing local/landing
mc mb --ignore-existing local/warehouse
echo "buckets ready"
