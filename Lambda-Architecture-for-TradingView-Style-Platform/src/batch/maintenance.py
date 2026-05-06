#!/usr/bin/env python3
"""
Iceberg table maintenance — compaction, manifest optimization,
snapshot expiration, and orphan file cleanup.

Usage:
    python maintenance.py
    python maintenance.py iceberg_catalog.crypto_lakehouse.coin_klines
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import (
    ICEBERG_TABLES,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    ORPHAN_FILE_RETENTION_HOURS,
    SNAPSHOT_RETENTION_HOURS,
    TARGET_FILE_SIZE_BYTES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

spark = (
    SparkSession.builder.appName("IcebergMaintenance")
    .config("spark.driver.memory",   "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg_catalog.type", "jdbc")
    .config("spark.sql.catalog.iceberg_catalog.uri",
            f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg_catalog.jdbc.user", os.environ.get("POSTGRES_USER", ""))
    .config("spark.sql.catalog.iceberg_catalog.jdbc.password", os.environ.get("POSTGRES_PASSWORD", ""))
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://cryptoprice/iceberg")
    .config("spark.sql.catalog.iceberg_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg_catalog.s3.endpoint", MINIO_ENDPOINT)
    .config("spark.sql.catalog.iceberg_catalog.s3.access-key-id", MINIO_ACCESS_KEY)
    .config("spark.sql.catalog.iceberg_catalog.s3.secret-access-key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.defaultCatalog", "iceberg_catalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def rewrite_data_files(table: str):
    log.info("[%s] RewriteDataFiles — compacting small files ...", table)
    result = spark.sql(f"""
        CALL iceberg_catalog.system.rewrite_data_files(
            table => '{table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'min-input-files',        '5',
                'max-concurrent-file-group-rewrites', '4'
            )
        )
    """)
    row = result.first()
    log.info("[%s] Compaction done: %d files rewritten into %d new files.",
             table,
             row["rewritten_data_files_count"] if row else 0,
             row["added_data_files_count"]     if row else 0)


def rewrite_manifests(table: str):
    log.info("[%s] RewriteManifests — optimizing metadata ...", table)
    spark.sql(f"CALL iceberg_catalog.system.rewrite_manifests(table => '{table}')")
    log.info("[%s] RewriteManifests done.", table)


def expire_snapshots(table: str):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SNAPSHOT_RETENTION_HOURS)
    log.info("[%s] ExpireSnapshots — removing snapshots older than %s ...", table, cutoff.isoformat())
    result = spark.sql(f"""
        CALL iceberg_catalog.system.expire_snapshots(
            table                => '{table}',
            older_than           => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}',
            retain_last          => 5,
            max_concurrent_deletes => 4
        )
    """)
    row = result.first()
    log.info("[%s] ExpireSnapshots done: %d data files, %d manifest files, %d manifest lists deleted.",
             table,
             row["deleted_data_files_count"]     if row else 0,
             row["deleted_manifest_files_count"] if row else 0,
             row["deleted_manifest_lists_count"] if row else 0)


def remove_orphan_files(table: str):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ORPHAN_FILE_RETENTION_HOURS)
    log.info("[%s] RemoveOrphanFiles — cleaning files older than %s ...", table, cutoff.isoformat())
    result = spark.sql(f"""
        CALL iceberg_catalog.system.remove_orphan_files(
            table      => '{table}',
            older_than => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
        )
    """)
    count = result.count()
    log.info("[%s] RemoveOrphanFiles done: %d orphan files removed.", table, count)


def maintain(table: str):
    log.info("===== START maintenance: %s =====", table)
    for op in (rewrite_data_files, rewrite_manifests, expire_snapshots, remove_orphan_files):
        try:
            op(table)
        except Exception as exc:
            log.error("[%s] %s failed: %s", table, op.__name__, exc)
    log.info("===== DONE maintenance: %s =====", table)


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ICEBERG_TABLES
    log.info("Iceberg maintenance started. Tables: %s", targets)
    for t in targets:
        maintain(t)
    log.info("All maintenance tasks completed.")
    spark.stop()
