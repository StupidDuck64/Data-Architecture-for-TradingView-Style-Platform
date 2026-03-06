#!/usr/bin/env python3
"""
backfill_influx.py
──────────────────
Tự động phát hiện khoảng trống dữ liệu trong InfluxDB (do tắt máy)
rồi fill lại bằng Binance klines REST API (interval=1m).

Chạy 1 lần khi khởi động (restart: "no" trong docker-compose).
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ─── Config ──────────────────────────────────────────────────────────────────
INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
KLINE_INTERVAL     = "1m"       
KLINE_BATCH        = 1000       
MIN_GAP_SEC        = 300        
MAX_BACKFILL_DAYS  = 7          
MAX_WORKERS        = 5          
REQUEST_DELAY      = 0.12

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill")


# ─── InfluxDB helpers ─────────────────────────────────────────────────────────

def wait_for_influx(client: InfluxDBClient, retries: int = 20, delay: float = 5.0):
    """Đợi InfluxDB sẵn sàng."""
    for i in range(retries):
        try:
            client.ping()
            log.info("InfluxDB ready.")
            return
        except Exception:
            log.info("Waiting for InfluxDB... (%d/%d)", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("InfluxDB not reachable after retries.")


def find_all_gaps(client: InfluxDBClient) -> dict[str, list[tuple[int, int]]]:
    """
    Tìm tất cả khoảng trống > MIN_GAP_SEC trong time series của từng symbol.
    Dùng elapsed() để detect gap ở BẤT KỲ đâu trong chuỗi (kể cả gap ở giữa).
    Trả về {symbol: [(gap_start_ms, gap_end_ms), ...]}
    """
    max_lookback_s = MAX_BACKFILL_DAYS * 24 * 3600
    min_gap_ms     = MIN_GAP_SEC * 1000

    # aggregateWindow(createEmpty:false) → chỉ giữ các window có data
    # elapsed() → tính khoảng cách giữa 2 window liên tiếp (per symbol)
    # Nếu elapsed_ms >> 60000 → có gap ở đó
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{max_lookback_s}s)
  |> filter(fn: (r) => r._measurement == "market_ticks"
      and r._field == "price"
      and (not exists r.source or r.source != "backfill"))
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> elapsed(unit: 1ms, columnName: "elapsed_ms")
  |> filter(fn: (r) => r.elapsed_ms > {min_gap_ms})
  |> keep(columns: ["symbol", "_time", "elapsed_ms"])
"""
    query_api = client.query_api()
    tables    = query_api.query(flux, org=INFLUX_ORG)

    result: dict[str, list[tuple[int, int]]] = {}
    for table in tables:
        for record in table.records:
            symbol     = record.values.get("symbol")
            elapsed_ms = int(record.values.get("elapsed_ms", 0))
            if not symbol or elapsed_ms <= 0:
                continue
            # _time = thời điểm data RESUME (cuối gap)
            # _time - elapsed_ms = thời điểm data STOP (đầu gap)
            gap_end_ms   = int(record.get_time().timestamp() * 1000)
            gap_start_ms = gap_end_ms - elapsed_ms + 60_000  # +1m = bỏ 1 window của aggregation
            result.setdefault(symbol, []).append((gap_start_ms, gap_end_ms))

    total_gaps = sum(len(v) for v in result.values())
    log.info("Detected %d gap(s) across %d symbol(s).", total_gaps, len(result))
    return result


# ─── Binance helpers ──────────────────────────────────────────────────────────

def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list[list]:
    """
    Lấy klines từ Binance REST API với phân trang tự động.
    Trả về list của [open_time, open, high, low, close, volume,
                      close_time, quote_volume, trades, ...]
    """
    all_klines: list[list] = []
    current_start = start_ms

    while current_start < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  KLINE_INTERVAL,
            "startTime": current_start,
            "endTime":   end_ms,
            "limit":     KLINE_BATCH,
        }
        try:
            resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=15)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            all_klines.extend(batch)
            last_open_time = int(batch[-1][0])
            if last_open_time <= current_start:
                break
            current_start = last_open_time + 60_000  # +1 phút
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            log.error("[%s] klines fetch error (start=%d): %s", symbol, current_start, e)
            break

    return all_klines


def klines_to_points(symbol: str, klines: list[list]) -> list[Point]:
    """Chuyển Binance klines → InfluxDB Points (cùng schema với Flink)."""
    points = []
    for k in klines:
        # k: [open_time, open, high, low, close, volume, close_time,
        #      quote_volume, trades, taker_buy_vol, taker_buy_quote, ignore]
        try:
            open_ms     = int(k[0])
            open_price  = float(k[1])
            close_price = float(k[4])
            volume      = float(k[5])
            quote_vol   = float(k[7])
            trades      = int(k[8])
            pct_change  = (close_price - open_price) / open_price * 100 if open_price else 0.0

            point = (
                Point("market_ticks")
                .tag("symbol",   symbol)
                .tag("exchange", "binance")
                .tag("source",   "backfill")        # phân biệt với realtime tick
                .field("price",             close_price)
                .field("bid",               close_price)   # không có từ klines, dùng close
                .field("ask",               close_price)   # không có từ klines, dùng close
                .field("volume",            volume)
                .field("quote_volume",      quote_vol)
                .field("price_change_pct",  pct_change)
                .field("trade_count",       trades)
                .time(open_ms, WritePrecision.MS)
            )
            points.append(point)
        except Exception as e:
            log.warning("[%s] skip kline row: %s", symbol, e)
    return points


# ─── Core logic ───────────────────────────────────────────────────────────────

def backfill_symbol(
    symbol: str,
    gap_start_ms: int,
    gap_end_ms: int,
    write_api,
) -> int:
    """
    Backfill 1 gap của 1 symbol.
    Trả về số điểm đã ghi.
    """
    gap_sec = (gap_end_ms - gap_start_ms) / 1000
    log.info(
        "[%s] Backfilling gap %.1f min: %s → %s",
        symbol,
        gap_sec / 60,
        datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
        datetime.fromtimestamp(gap_end_ms   / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
    )

    klines = fetch_klines(symbol, gap_start_ms, gap_end_ms)
    if not klines:
        log.warning("[%s] No klines returned for this gap.", symbol)
        return 0

    points = klines_to_points(symbol, klines)
    if not points:
        return 0

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        log.info("[%s] Written %d points.", symbol, len(points))
        return len(points)
    except Exception as e:
        log.error("[%s] write error: %s", symbol, e)
        return 0


def run():
    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    wait_for_influx(client)

    # find_all_gaps dùng elapsed() → phát hiện gap ở BẤT KỲ đâu trong chuỗi
    all_gaps = find_all_gaps(client)
    if not all_gaps:
        log.info("No gaps detected — InfluxDB is complete.")
        client.close()
        return

    # Flat list: [(symbol, start_ms, end_ms), ...]
    tasks = [
        (sym, start, end)
        for sym, gaps in all_gaps.items()
        for (start, end) in gaps
    ]
    log.info("Total tasks: %d gaps to fill.", len(tasks))

    total_points = 0
    total_filled = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(backfill_symbol, sym, start, end, write_api): (sym, start, end)
            for (sym, start, end) in tasks
        }
        for future in as_completed(futures):
            sym, start, end = futures[future]
            try:
                n = future.result()
                if n > 0:
                    total_points += n
                    total_filled += 1
            except Exception as e:
                log.error("[%s] Unexpected error: %s", sym, e)

    log.info(
        "Backfill complete — %d/%d gaps filled, %d total points written.",
        total_filled, len(tasks), total_points,
    )
    client.close()


if __name__ == "__main__":
    run()
