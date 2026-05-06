#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
from typing import List, Optional

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    get_dagster_logger,
)

PROJECT_DIR = Path(os.environ.get("CRYPTO_PROJECT_DIR", "/app"))

SPARK_HOME = Path(os.environ.get("SPARK_HOME", "/opt/spark"))

SPARK_EVENTS_DIR = Path(os.environ.get("SPARK_EVENTS_DIR", "/opt/spark-events"))

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.2",
])

def _run_spark_job(context: AssetExecutionContext, script_name: str, extra_args: Optional[List[str]] = None) -> None:
    logger = get_dagster_logger()
    script_path = PROJECT_DIR / "src" / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    SPARK_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(SPARK_HOME / "bin" / "spark-submit"),
        "--master", SPARK_MASTER,
        "--packages", SPARK_PACKAGES,
        "--conf", "spark.eventLog.enabled=true",
        "--conf", f"spark.eventLog.dir=file://{SPARK_EVENTS_DIR}",
        str(script_path),
    ]

    if extra_args:
        cmd.extend(extra_args)

    logger.info("Running command: %s", " ".join(cmd))

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,    # merge stderr into stdout for a single stream
        text=True,
        bufsize=1,                   # line-buffered
        cwd=str(PROJECT_DIR),
    )

    for line in process.stdout:
        line = line.rstrip()
        if line:
            logger.info(line)

    process.wait()

    if process.returncode != 0:
        raise Exception(
            f"Spark job '{script_name}' failed with exit code {process.returncode}. "
            f"Check logs in Dagster UI or Spark History Server (http://localhost:18080)."
        )

    logger.info("Spark job '%s' completed successfully.", script_name)


@asset(
    description=(
        "Pulls 1h OHLCV klines from Binance API for all USDT pairs, "
        "fetches only rows newer than the last run, then writes to Iceberg "
        "table. Also detects & fills InfluxDB gaps from machine downtime."
    ),
    group_name="ingestion",
)
def backfill_historical(context: AssetExecutionContext) -> None:
    _run_spark_job(
        context,
        script_name="batch/backfill.py",
        extra_args=["--mode", "all", "--iceberg-mode", "incremental"],
    )


@asset(
    description=(
        "Aggregates 1-minute candles into hourly candles to reduce data bloat. "
        "Runs on both InfluxDB (candles measurement) and Iceberg (coin_klines table). "
        "Deletes 1m data older than RETENTION_1M_DAYS (default: 90 days)."
    ),
    group_name="maintenance",
)
def aggregate_candles(context: AssetExecutionContext) -> None:
    _run_spark_job(
        context,
        script_name="batch/aggregate.py",
        extra_args=["--mode", "all"],
    )


@asset(
    description=(
        "Runs 4 maintenance tasks on Iceberg tables: "
        "(1) Compact small files into ~128 MB files, "
        "(2) Rewrite manifests to reduce metadata overhead, "
        "(3) Expire snapshots older than 48 hours, "
        "(4) Remove orphan files no longer referenced."
    ),
    group_name="maintenance",
)
def iceberg_table_maintenance(context: AssetExecutionContext) -> None:
    _run_spark_job(
        context,
        script_name="batch/maintenance.py",
    )


schedule_weekly_maintenance = ScheduleDefinition(
    name="weekly_iceberg_maintenance",
    target=iceberg_table_maintenance,
    cron_schedule="0 3 * * 0",
    description="Runs every Sunday at 03:00 AM to compact and clean up Iceberg tables.",
)

schedule_daily_aggregate = ScheduleDefinition(
    name="daily_candle_aggregation",
    target=aggregate_candles,
    cron_schedule="0 4 * * *",
    description="Runs daily at 04:00 AM to aggregate 1m candles into 1h and clean up old 1m data.",
)

defs = Definitions(
    assets=[
        backfill_historical,
        aggregate_candles,
        iceberg_table_maintenance,
    ],
    schedules=[
        schedule_weekly_maintenance,
        schedule_daily_aggregate,
    ],
)
