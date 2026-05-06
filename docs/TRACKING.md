# TRACKING — AI Assistant Working Document

> **Purpose:** Personal reference for AI assistant to maintain context across sessions.  
> **Last updated:** 2026-04-28

---

## 1. Project Overview

**Lambda Architecture for TradingView-Style Platform** — real-time crypto price monitoring and charting platform using Lambda Architecture (speed + batch + serving layers).

- **Repo:** `StupidDuck64/Lambda-Architecture-for-TradingView-Style-Platform`
- **Latest commit:** `0802fe0` (main)
- **Deployment target:** AWS t3a.2xlarge (8 vCPU, 32GB RAM, 100GB gp3)
- **Docker services:** 21 containers via `docker-compose.yml`
- **Data sources:** Binance WebSocket API (~400 USDT pairs)

📄 **For full technical details, see [DOCUMENTATION.md](./DOCUMENTATION.md)**  

### Quick Reference — Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Message broker | Apache Kafka (KRaft) | 3.9.0 |
| Schema registry | Apicurio | 2.6.2 |
| Stream processing | Apache Flink (PyFlink) | 1.18.1 (Python 3.10.12) |
| Batch processing | Apache Spark | 3.5 (Python 3.10) |
| Hot cache | KeyDB | latest |
| Time-series DB | InfluxDB | 2.7 |
| Cold storage | Iceberg + MinIO | 1.5.2 + latest |
| Federated query | Trino | 442 |
| Orchestration | Dagster | latest (Python 3.8.10) |
| API server | FastAPI + Uvicorn | 0.115+ (Python 3.11-slim) |
| Producer | Python WebSocket | Python 3.11-slim |
| Backfill | Python requests | Python 3.14-slim |
| Frontend | React 19 + lightweight-charts | v5.1.0 |
| CSS framework | TailwindCSS | 3.4.4 |
| Language | TypeScript (strict) | 5.7+ |
| Build tool | Vite | 6.4 |
| Reverse proxy | Nginx | 1.27 |
| Metadata DB | PostgreSQL | 16 |

### Quick Reference — Key File Sizes

| File | Lines | Role |
|---|---|---|
| `src/processing/pipeline.py` | ~210 | Flink job entry point (writers are split into modules) |
| `src/batch/backfill.py` | ~510 | Multi-mode backfill (Spark/direct) |
| `src/producer/main.py` | ~260 | Exchange-agnostic WS → Kafka producer |
| `frontend/src/components/CandlestickChart.tsx` | ~1020 | Main chart component (TypeScript) |
| `frontend/src/services/marketDataService.ts` | ~388 | Frontend API service layer |
| `backend/services/candle_service.py` | ~280 | Core OHLCV business logic (shared) |
| `backend/api/klines.py` | ~170 | OHLCV REST endpoint (thin handler) |
| `backend/api/historical.py` | ~90 | Historical range queries |
| `backend/api/websocket.py` | ~135 | WebSocket real-time stream |
| `src/lakehouse/pipeline.py` | ~220 | Spark Streaming → Iceberg |
| `frontend/src/App.tsx` | ~240 | Main React app layout |
| `src/batch/maintenance.py` | ~130 | Iceberg compaction |
| `src/batch/aggregate.py` | ~120 | Spark 1m→1h aggregation |
| `orchestration/assets.py` | 151 | Dagster assets + schedules |

### Quick Reference — Directory Layout

```
project-root/
├── backend/                   # FastAPI API layer (MVC architecture)
│   ├── app.py                 # App entry point, router registration
│   ├── api/                   # Route handlers (thin controllers)
│   │   ├── health.py, klines.py, historical.py, websocket.py
│   │   ├── ticker.py, orderbook.py, trades.py, symbols.py, indicators.py
│   ├── core/                  # Config, constants, database connections
│   │   ├── config.py, constants.py, database.py
│   ├── services/              # Business logic layer
│   │   └── candle_service.py    # Shared OHLCV logic (validate, aggregate, query)
│   └── models/                # Pydantic response models
│       ├── candle.py, ticker.py, health.py
├── src/                       # Data processing layer
│   ├── common/                # Shared config, Kafka, Avro, logging
│   ├── exchanges/             # Exchange abstraction (binance/ implementations)
│   ├── producer/              # Kafka quad-stream producer
│   ├── processing/            # Flink pipeline and writers
│   ├── lakehouse/             # Spark structured streaming to Iceberg
│   └── batch/                 # Historical backfill and maintenance jobs
├── frontend/                  # React 19 SPA (Vite + TypeScript)
│   └── src/
│       ├── components/        # 16 components (.tsx) + chart/ subdir
│       │   └── chart/         # chartConstants.ts, indicatorUtils.ts, OHLCVBar.tsx, etc.
│       ├── services/          # marketDataService.ts, symbolMetaService.ts
│       ├── hooks/             # useApiCall.ts, useSymbolMeta.ts
│       ├── contexts/          # AuthContext.tsx
│       ├── i18n/              # translations.ts, index.tsx (~130 keys, en + vi)
│       ├── types/             # index.ts (shared TS interfaces)
│       ├── data/              # fallbackSymbolMeta.ts (~90 symbols)
│       └── utils/             # storageHelpers.ts, errors.ts
├── orchestration/             # Dagster assets + workspace.yaml
├── schemas/                   # 4 Avro schemas (ticker, kline, trade, depth)
├── config/                    # spark-defaults.conf
├── docker/                    # Dockerfiles (9 subdirs)
├── scripts/                   # Shell scripts
├── tests/                     # pytest (unit/, integration/, e2e/, security/, performance/)
├── docs/                      # Documentation
├── docker-compose.yml         # 21 services (base)
├── docker-compose.override.yml # Dev mode overrides
├── docker-compose.prod.yml    # Production overrides
├── Makefile                   # Dev/prod/test convenience targets
├── pyproject.toml             # Pytest config
└── .env.example               # Environment template
```

---

## 2. Operating Principles

> These are the rules I must follow when making changes to the project. The owner can modify these at any time.

### 2.1 General Rules

1. **Always update this TRACKING.md** after completing any task — add to changelog, update file sizes if changed, note any new patterns or gotchas discovered.
2. **Read this file first** at the start of every session to re-establish context.
3. **Preserve existing comments and docstrings** unless the user explicitly says to modify them.
4. **Follow existing code patterns** — match the style, naming conventions, and structure already in use.
5. **Incremental refactoring** — refactoring is crucial but can break functional code. Treat it step-by-step and double-check to ensure new code runs as performant or better without breaking anything major.

### 2.2 Code Style & Patterns

1. **Python (backend):** Follow existing patterns in the codebase:
   - MVC architecture in `backend/` (`api/`, `services/`, `models/`, `core/`)
   - Route handlers in `backend/api/` should be thin, delegating logic to `backend/services/`
   - Data validation and response serialization using Pydantic models in `backend/models/`
   - Singleton connections in `backend/core/database.py`
   - Environment variables read from `backend/core/config.py`
   - Flux queries for InfluxDB, SQL for Trino
   - Avro serialization for Kafka (Confluent wire format)
2. **TypeScript (frontend):** Follow existing patterns:
   - React 19 functional components with hooks (`.tsx` extension, strict TypeScript)
   - Vite 6 for build tooling (`import.meta.env` for environment variables)
   - All props and state must have explicit TypeScript interfaces
   - TailwindCSS for styling
   - lightweight-charts v5.1.0 API (use `any` refs for series instances)
   - `marketDataService.ts` as the single API service layer
   - `symbolMetaService.ts` for dynamic symbol metadata (CoinGecko + localStorage 24h cache)
   - Centralized error handling via `useApiCall` hook + `ToastProvider`
   - Full i18n via `useI18n()` hook — no hardcoded user-facing strings
   - Time convention: lightweight-charts uses seconds, API uses milliseconds
   - Shared types live in `src/types/index.ts`
3. **Python Versions & Compatibility:** Keep the services compatible with their respective environments:
   - **Producer/FastAPI:** Standardized on Python 3.11. Avoid upgrading to 3.12+ until dependencies like `fastavro` and `kafka-python` have been verified or replaced.
   - **Flink/Spark:** Rely on the bundled Python versions (3.10) in their respective Docker images. Do not force Python 3.12 on PyFlink 1.18 due to `distutils` removal.
4. **Docker:** Changes to services must be reflected in `docker-compose.yml`. Use existing build patterns. For local development, prefer using `make dev` and `make prod` workflows.

### 2.3 Language

- **Documentation** is written in Vietnamese (the project owner's preference). Match the existing language.
- **Code comments** can be in English or Vietnamese, matching what's already in each file.
- **Commit messages** are in English.

### 2.4 Testing & Verification

1. Before finalizing changes, verify the logic is consistent across layers (e.g., Flink writer → KeyDB key → FastAPI reader → Frontend consumer).
2. If modifying API endpoints, ensure frontend `marketDataService.ts` is updated accordingly.
3. If changing docker-compose, verify dependencies and health checks are correct.

### 2.5 Key Gotchas to Remember

1. **Time units:** lightweight-charts uses seconds, all backend APIs use milliseconds. Frontend converts: `openTime / 1000 → time`.
2. **Timeframe casing:** Frontend uses uppercase `1H`, `4H`, `1D`, `1W` but `.toLowerCase()` before API calls.
3. **KeyDB dedup:** Sorted sets deduplicate by `(score, member)` pair — always `ZREMRANGEBYSCORE` before `ZADD` for klines.
4. **Ticker staleness:** `!ticker@arr` has 14–30s delay. Only use ticker to enrich candle close if `ticker.event_time > last_1s_candle.kline_start`.
5. **WS vs Poll coordination:** WS is authoritative for live bar (1m+), poll should skip the last candle to avoid flicker.
6. **InfluxDB scroll-left:** Must use absolute `range(start: RFC3339, stop: RFC3339)` for `endTime` queries, not relative `range(start: -Nh)`.
7. **Flink safety timer:** Must cancel old timer before registering new one (KlineWindowAggregator).
8. **Frontend chart re-render:** Use `.update()` for single bar updates, `.setData()` only for bulk operations (initial load, scroll-left merge).
9. **Producer WebSocket limit:** Max 200 symbols per WS connection to avoid Binance 502.

### 2.6 Rebuild Commands After Code Changes

| Changed | Command |
|---|---|
| `backend/` | `make dev` or `docker compose up -d --build fastapi` |
| `frontend/` | `make dev` or `docker compose up -d --build nginx` |
| `src/` (Flink job) | Cancel running Flink job, re-submit via REST |
| `src/` (Spark job) | Re-submit via `spark-submit` |
| `docker/` | `docker compose up -d --build <service>` |
| Test execution | `make test` or `make test-all` |

---

## 3. Current State & Notes

### Known Stable Commit
- `0802fe0` — Latest known working commit before refactoring.

### Active Configuration
- Flink parallelism: 1
- Flink TaskManager slots: 2
- TaskManager memory: 6144m (cap 7168m)
- KeyDB maxmemory: 2560mb
- InfluxDB 1m retention: 90 days
- KeyDB 1s TTL: 8h (28800s)
- KeyDB 1m TTL: 7d (604800s)
- Kafka retention: 48h
- Dagster schedules: daily 04:00 (aggregate), weekly Sunday 03:00 (iceberg maintenance)
- HTTPS: Let's Encrypt via certbot + DuckDNS dynamic DNS

### Frontend Component Tree (current)
```
App.tsx (TradingDashboard)
├── ErrorBoundary.tsx
├── ToastProvider.tsx
├── I18nProvider (i18n/index.tsx)
├── AuthContext.Provider (contexts/AuthContext.tsx)
├── Header.tsx
│   ├── Navigation drawer
│   └── LanguageSwitcher.tsx
├── DrawingToolbar.tsx
│   └── ToolSettingsPopup.tsx
├── CandlestickChart.tsx (~1020 lines — CORE)
│   ├── MarketSelector.tsx (+ useSymbolMeta)
│   ├── DateRangePicker.tsx
│   ├── chart/IndicatorPanel.tsx
│   ├── chart/OHLCVBar.tsx
│   ├── chart/OscillatorPane.tsx
│   ├── chart/chartConstants.ts
│   ├── chart/indicatorUtils.ts
│   ├── ChartOverlay.tsx (drawings)
│   ├── OrderBook.tsx
│   └── RecentTrades.tsx
├── Watchlist.tsx (+ useSymbolMeta)
├── OverviewChart.tsx (+ useSymbolMeta)
├── SystemHealthCard.tsx
└── AuthModal.tsx
```

### Frontend Type System
```
src/types/index.ts:
  Candle, RawCandle, Ticker, OrderBookEntry, OrderBookData, Trade,
  SymbolInfo, SymbolMeta, WatchlistItem, HistoricalRange, HealthData,
  IndicatorSettings, DrawingPoint, Drawing, TooltipData,
  UserSession, AuthResult, Timeframe, WatchlistFilter
```

### Frontend Build Output
```
dist/index.html                   0.74 kB │ gzip:   0.40 kB
dist/assets/index-*.css          22.77 kB │ gzip:   4.77 kB
dist/assets/index-*.js          471.79 kB │ gzip: 146.62 kB
```

---

## 4. Changelog

All changes made by AI assistant, in reverse chronological order.

### 2026-05-05 — Session 8: Multi-Timeframe Candles & Historical Mode

**Task:** Implement end-to-end multi-timeframe support (1s, 1m, 5m, 15m, 1h, 4h, 1d, 1w) with historical date range picker and improved data aggregation logic.

**Changes:**

**Backend:**
1. **`backend/services/candle_service.py` (+85 lines)** — Major improvements:
   - Added `normalize_interval()`: Normalize interval to lowercase (1M → 1m, "  1H  " → 1h)
   - Added `interval_to_seconds()`: Convert interval string to seconds, returns 0 if invalid
   - Added `interval_to_ms()`: Convert interval string to milliseconds
   - Fixed `aggregate()` function (CRITICAL BUG FIX):
     * Old behavior: Used input order for open/close (WRONG)
     * New behavior: Sorts each bucket by timestamp, uses earliest for open, latest for close
     * Handles out-of-order input correctly
     * Deduplicates via `merge_unique()` first
   - Enhanced `merge_unique()` with quality prioritization:
     * Priority 1: Closed/final candle (is_closed flag)
     * Priority 2: Higher volume (more complete data)
     * Priority 3: If equal, prefer incoming (latest)
   - Added `_is_better_candle()` helper for quality comparison

2. **`backend/api/klines.py` (+30 lines)** — Enhanced enrichment logic:
   - Improved `_enrich_with_live_ticker()` with staleness checking
   - Now queries source interval (1m) to verify ticker freshness against sub-candle data
   - Only enriches if ticker is fresher than BOTH latest candle AND latest sub-candle
   - Prevents stale ticker from overwriting fresh data

3. **`tests/unit/test_candle_service.py` (+150 lines)** — Comprehensive new tests (14 new tests):
   - `TestNormalizeInterval`: 3 tests for lowercase/uppercase/whitespace normalization
   - `TestIntervalToSeconds`: 3 tests for valid/uppercase/invalid intervals
   - `TestIntervalToMs`: 2 tests for millisecond conversion
   - `TestMergeUnique` enhancements:
     * `test_overlap_prefers_closed_candle`: Closed wins over partial
     * `test_overlap_prefers_higher_volume`: Higher volume wins if both partial
     * `test_idempotent_replay`: Replaying same batch is idempotent
     * `test_out_of_order_merge`: Out-of-order candles sorted correctly
   - `TestAggregate` enhancements:
     * `test_aggregate_out_of_order_input`: open/close based on timestamps, not input order
     * `test_aggregate_duplicate_timestamps`: Dedup before aggregating
     * `test_aggregate_1m_to_15m`: 1m → 15m aggregation (15 candles → 1 bar)
     * `test_aggregate_1m_to_1h`: 1m → 1h aggregation (60 candles → 1 bar)
     * `test_aggregate_multiple_buckets`: Creates multiple buckets correctly
     * `test_aggregate_preserves_closed_flag_priority`: Respects merge_unique priority

4. **`tests/integration/test_candle_idempotency.py` (NEW, 120 lines)** — Integration tests:
   - `test_partial_vs_closed_candle_merge`: Prefer closed over partial
   - `test_replay_idempotency`: Same batch replayed is idempotent
   - `test_mixed_quality_candles`: Quality-based selection works
   - `test_staleness_check_logic`: Staleness check prevents old ticker overwriting fresh data

**Frontend:**
1. **`frontend/src/services/marketDataService.ts` (+9 lines)** — Interval normalization:
   - `fetchCandles()`: Normalize interval to lowercase before API call
   - `subscribeCandle()`: Normalize interval for WebSocket connection
   - `fetchHistoricalCandles()`: Normalize interval before sending to API
   - Reason: UI uses uppercase (1H, 4H, 1D, 1W), backend expects lowercase (1h, 4h, 1d, 1w)

2. **`frontend/src/components/CandlestickChart.tsx` (+145 lines)** — Historical mode implementation:
   - Added state: `isLiveMode`, `historicalRange`, `unsubscribeRef`, `pollIntervalRef`, `historicalRequestIdRef`
   - Added `handleHistoricalRange()`: Request handler for date range selection with request ID tracking
   - Added `handleBackToLive()`: Return to live mode
   - Enhanced `useEffect` for live mode: Properly manages WebSocket subscription and poll interval refs
   - Added historical mode handler: Only refetch when symbol/timeframe changes in historical mode
   - Enhanced timeframe buttons: Disable 1s in historical mode (not available in historical)
   - Fixed 1s → 1m auto-switch when entering historical mode
   - Request cancellation: Uses request ID to prevent race conditions (newer request invalidates older)

**Infrastructure:**
- `backfill_90days.log`: Log file from InfluxDB backfill process

**Test Coverage:**
| Category | Before | Added | Total |
|---|---|---|---|
| Unit | 40 | 14 | 54 |
| Integration | 0 | 4 | 4 |
| **Total** | **40** | **18** | **58** |

**Key Bug Fixes:**
1. **Aggregate function (CRITICAL)**: Now handles out-of-order input correctly
   - Example: Input `[candle_120s, candle_0s, candle_60s]` now correctly uses open from 0s, close from 120s
2. **Ticker enrichment staleness**: Prevents stale ticker from overwriting fresh candle data
3. **Interval normalization**: Ensures frontend uppercase intervals (1H) work correctly with lowercase backend (1h)
4. **Historical mode**: Added request ID tracking to prevent race conditions during mode switching

**Technical Debt Addressed:**
- Merge logic now respects data quality (closed > high volume > partial)
- Aggregate logic is timestamp-based, not input-order-based
- Interval handling is centralized and normalized
- Historical mode doesn't block live mode data updates

**Deferred (Future PRs):**
- Physical materialization of 5m/15m/1h/4h/1d/1w in Flink/Spark (still uses aggregate fallback)
- Iceberg MERGE/upsert for idempotent batch writes
- Performance benchmarks for large historical ranges

**Notes/Gotchas discovered:**
- Aggregate function bug would only manifest with out-of-order 1m candles → higher intervals. Most queries work by coincidence if 1m candles arrive in order.
- Request ID pattern prevents race conditions when switching between live/historical modes rapidly
- Interval normalization must happen at both API request (frontend) and response parsing (backend)
- Closed candle priority is essential for correctness when same timestamp appears in multiple sources

**Impact:**
- Backend: Fix for critical aggregation bug + new interval helpers
- Frontend: Full historical mode with date range picker + interval normalization
- Tests: 18 new tests covering multi-timeframe logic and data quality
- Data correctness: All intervals (1s-1w) now supported end-to-end

### 2026-05-02 — Session 7: Comprehensive Test Suite

**Task:** Implement the remaining tests across all 5 test categories (unit, integration, e2e, security, performance).

**Changes:**
1. **Unit Tests — Constants (`test_constants.py`, 11 tests)** — Interval definitions, symbol regex validation, data limit sanity checks, and hourly interval groupings.
2. **Unit Tests — Binance Mappers (`test_binance_mappers.py`, 12 tests)** — Ticker, aggregate trade, kline, and depth message conversion from raw Binance JSON to canonical format.
3. **Unit Tests — Binance Client (`test_binance_client.py`, 12 tests)** — Stream name builders, URL construction, default/custom config, and mapper delegation.
4. **Unit Tests — Models Extended (`test_models_extended.py`, 16 tests)** — Edge cases for all Pydantic models: zero/large values, type coercion, JSON roundtrip, TradeResponse, OrderBookEntry, degraded health states.
5. **Integration Tests — Health API (`test_api_health.py`, 5 tests)** — All-OK, degraded states (Redis/InfluxDB/Trino down), and latency metrics.
6. **Integration Tests — Ticker API (`test_api_ticker.py`, 5 tests)** — Single ticker retrieval, 404, case normalization, all-tickers listing, empty state.
7. **Integration Tests — Symbols API (`test_api_symbols.py`, 5 tests)** — USDT/BTC pair formatting, unknown quote, sort order, empty state.
8. **Integration Tests — Trades API (`test_api_trades.py`, 5 tests)** — Data retrieval, side detection, 404, limit bounds.
9. **Integration Tests — Indicators API (`test_api_indicators.py`, 4 tests)** — Full/partial data, 404, case normalization.
10. **Integration Tests — Klines API (`test_api_klines.py`, 7 tests)** — Symbol/interval validation, limit bounds, cache hit, 1s source, missing params.
11. **Integration Tests — Historical API (`test_api_historical.py`, 8 tests)** — Date range validation, overflow timestamps, empty result.
12. **Security Tests — API Security (`test_api_security.py`, 8 tests)** — SQL injection through endpoints, XSS, path traversal, CORS, oversized queries, null bytes, emoji.
13. **Performance Tests — Benchmarks (`test_benchmarks.py`, 9 tests)** — Aggregation (10K/50K candles), merging (10K/overlap), validation/conversion batches with explicit time limits.
14. **E2E Tests — App Routes (`test_app_routes.py`, 6 tests)** — Route registration, app metadata, OpenAPI schema, docs endpoint, 404, router tags.
15. **Infrastructure** — Created `tests/integration/`, `tests/e2e/`, `tests/performance/` packages with `__init__.py`.

**Test counts:**
| Category    | Before | Added | Total |
|---|---|---|---|
| Unit        | 29     | 51    | 80    |
| Integration | 0      | 39    | 39    |
| Security    | 9      | 8     | 17    |
| Performance | 0      | 9     | 9     |
| E2E         | 0      | 6     | 6     |
| **Total**   | **40** | **121** | **161** |

**Notes/Gotchas discovered:**
- WebSocket routes (`@router.websocket`) don't appear in the OpenAPI schema — FastAPI excludes them from `/openapi.json` paths since WS uses a different protocol.
- `pytest-asyncio` 1.x with `asyncio_mode = "auto"` triggers a config warning on newer pytest; consider upgrading to `pytest-asyncio>=0.21` for `mode = "auto"` support.
- Python 3.14 on Windows uses `py` launcher, not `python`. Test commands should use `py -m pytest`.

**Impact:**
- Testing: 121 new tests, 161 total (all passing). Coverage now spans all backend layers: models, services, API endpoints, exchange abstraction, and constants.


### 2026-04-28 — Session 6: Infrastructure & Pipeline Restoration

**Task:** Fix the broken data pipeline, resolving Docker build errors, network bindings, and Flink `RESTARTING` loops.

**Changes:**
1. **Producer Image Rebuild & Python Downgrade** — The `producer` container failed to start due to a stale code reference (`src/producer_binance.py`). During the rebuild, compilation of `fastavro` failed under `python:3.14-slim`. Downgraded the `producer` Dockerfile to `python:3.11-slim` to ensure C-extension compatibility.
2. **Nginx Port Conflict** — `dagster-webserver` and `nginx` both attempted to bind to host port `3000`. Removed the `3000:80` mapping for `nginx` in `docker-compose.override.yml`, as Nginx already serves frontend/API traffic correctly on port `80`.
3. **Binance WebSocket Timeout** — The Flink job was missing the `crypto_ticker` Kafka topic because the `!ticker@arr` stream silently timed out. Switched the `WS_TICKER_URL` to `!miniTicker@arr` in `src/exchanges/binance/client.py`, which is lighter and reliably connects.
4. **Flink Dependency Distribution** — The Flink job crashed with `ModuleNotFoundError: No module named 'processing'`. Updated `scripts/auto_submit_jobs.sh` to correctly pass `--pyFiles /app/src` so the Flink TaskManagers have access to the local Python modules.

**Notes/Gotchas discovered:**
- When a folder structure changes (e.g., `src/producer/main.py`), Docker images must be rebuilt. `docker compose up -d` does not automatically rebuild images unless `--build` is passed.
- Python 3.12+ compatibility is a major concern. Flink 1.18 relies on the deprecated `distutils`, and older libraries like `kafka-python` fail entirely on Python 3.12. Stick to Python 3.11 for safety.
- Binance `!ticker@arr` stream might time out or be rate-limited depending on payload size/region; `!miniTicker@arr` is a safer alternative.

**Impact:**
- Infrastructure: Restored stability across producer, Flink, and frontend networking. Data correctly flows into KeyDB and InfluxDB again.

### 2026-04-28 — Session 5: Frontend TypeScript Migration

**Task:** Complete migration of the frontend from JavaScript/JSX to TypeScript/TSX. Upgrade React 18→19. Add centralized error handling, dynamic symbol metadata service, and full i18n coverage.

**Changes:**
1. **TypeScript Toolchain** — Added `tsconfig.json` (strict mode), `tsconfig.node.json`, `vite-env.d.ts`. Updated `package.json` with React 19, TypeScript 5.7+, `@types/react` 19.
2. **Type Definitions (`src/types/index.ts`)** — Created 18 shared interfaces: `Candle`, `Ticker`, `Drawing`, `IndicatorSettings`, `HistoricalRange`, etc.
3. **Core Service Migration** — `marketDataService.js` → `.ts`, `storageHelpers.js` → `.ts`, `translations.js` → `.ts` (TranslationKey type), `AuthContext.jsx` → `.tsx`, `i18n/index.jsx` → `.tsx`.
4. **Centralized Error Handling** — Created `src/utils/errors.ts` (AppError hierarchy), `src/hooks/useApiCall.ts` (generic fetcher with retry), `src/components/ToastProvider.tsx` (toast notification system). Wired into `index.tsx`.
5. **Dynamic Symbol Metadata** — Created `src/services/symbolMetaService.ts` (CoinGecko API + 24h localStorage cache), `src/data/fallbackSymbolMeta.ts` (~90 symbols), `src/hooks/useSymbolMeta.ts`. Integrated into MarketSelector, OverviewChart, Watchlist.
6. **Component Migration (20 files)** — Migrated all 16 components + 4 chart sub-modules from `.jsx`/`.js` to `.tsx`/`.ts`. Added typed props, state, and refs throughout. Key files: CandlestickChart (~1020 lines), ChartOverlay (~430 lines).
7. **i18n Completion** — Added ~50 new translation keys (en + vi). Replaced all hardcoded English and Vietnamese strings with `t()` calls. Total: ~130 keys.
8. **Entry Points** — `index.jsx` → `index.tsx`, `App.jsx` → `App.tsx`.
9. **Nginx** — Updated asset caching from `/static/` to `/assets/` (Vite output path). Added font/image caching rules.
10. **Build Verification** — `tsc --noEmit` = 0 errors. `vite build` succeeds (471.79 kB JS gzipped to 146.62 kB).

**Deleted files:** All `.jsx` and `.js` source files in `frontend/src/` (replaced by `.tsx`/`.ts` counterparts).

**Notes/Gotchas discovered:**
- lightweight-charts v5 `lineWidth` only accepts integer union `1 | 2 | 3 | 4`, not floats like `1.5`. Must cast.
- lightweight-charts `time` must be cast to `UTCTimestamp` when passing raw `number` values.
- `tsconfig.json` `references` to `tsconfig.node.json` requires `composite: true` on the referenced config, which conflicts with `noEmit: true`. Simplest fix: remove the reference since it's only for `vite.config.ts`.
- `IndicatorSettings` should be a flexible per-indicator interface (with index signature) rather than a rigid top-level structure, since components access it as `Record<string, IndicatorSettings>`.

**Impact:**
- Frontend: Complete rewrite — 27 files migrated, 7 new files created, all legacy deleted.
- Infrastructure: nginx.conf asset caching updated.
- Docs: TRACKING.md and DOCUMENTATION.md updated.

### 2026-04-25 — Session 4: Data Processing Layer Refactoring

**Task:** Refactor monolithic `src/` folder into a clean modular architecture, preparing for future multi-exchange support.

**Changes:**
1. **Exchange Abstraction (`src/exchanges/`)** — Created `ExchangeClient` base class and `BinanceClient` implementation. Replaced hardcoded Binance WS/REST endpoints.
2. **Shared Infrastructure (`src/common/`)** — Centralized `config.py` (eliminated 15+ duplicated env blocks), extracted thread-safe `kafka_client.py` and `avro_serializer.py`.
3. **Producer Service (`src/producer/`)** — Rewrote the 632-line monolith into a ~250-line exchange-agnostic orchestrator. Upgraded container to Python 3.14.
4. **Flink Pipeline (`src/processing/`)** — Split the 996-line monolith into `pipeline.py` + 7 individual writer modules (e.g., `keydb_ticker.py`, `kline_aggregator.py`).
5. **Batch Jobs (`src/batch/`)** — Renamed and refactored maintenance and backfill jobs. Translated Vietnamese docstrings to English in `backfill.py`.
6. **Lakehouse (`src/lakehouse/`)** — Cleaned up the Spark structured streaming pipeline.
7. **Infrastructure Updates** — Updated `orchestration/assets.py`, `scripts/auto_submit_jobs.sh`, and `docker-compose.yml` to reflect new paths.

**Notes/Gotchas discovered:**
- When refactoring PyFlink streams, writer logic inside `FlatMapFunction` or `KeyedProcessFunction` MUST read environment variables natively inside the `open()` method, as importing module-level envs from other files causes serialization issues across the Flink cluster.

**Impact:**
- Data Processing: Complete restructuring of `src/` from 7 monolithic files to 20+ cleanly separated modules.
- Infra: Docker path updates.



### 2026-04-25 — Session 3: CRA to Vite Migration & Python 3.14 Upgrade

**Task:** Migrate frontend from Create React App to Vite, upgrade backend Python from 3.11 to 3.14, and update TRACKING principles.

**Changes:**
1. **Frontend Migration:** Removed `react-scripts`, added `vite` and `@vitejs/plugin-react`. Renamed all 21 React component files from `.js` to `.jsx`. Updated `package.json`, created `vite.config.js`, moved `index.html` to root, converted tailwind/postcss configs to ESM. Updated environment variables to `VITE_` prefix and `import.meta.env`. Build time improved significantly (~2.5s).
2. **Backend Upgrade:** Updated FastAPI Dockerfile to `python:3.14-slim`. Cleaned up `from __future__ import annotations` across the backend files while maintaining compatibility for Pydantic models with `Optional[]` type hints.
3. **Docs Update:** Updated `docs/TRACKING.md` principles to reflect `backend/` MVC architecture, Vite frontend, and Docker Make command patterns.

**Notes/Gotchas discovered:**
- When migrating to Vite, explicitly renaming files containing JSX to `.jsx` is required for Vite's esbuild transform to work without extra configuration.
- Local tests use Python 3.9 where `X | None` syntax throws an error on type hints without `from __future__ import annotations`. However, Pydantic evaluates annotations at class definition time, so `Optional[float]` must be used instead for models.

**Impact:**
- Frontend: `package.json`, Vite configs, component extensions, Dockerfile path.
- Backend: Dockerfile base image, removed 6 redundant imports.
- Docs: Updated TRACKING.md principles.

### 2026-04-25 — Session 1: Initial Setup

**Task:** Create TRACKING.md and update DOCUMENTATION.md

**Changes:**
1. **Created `docs/TRACKING.md`** (this file) — AI assistant working document with:
   - Project overview & quick references
   - Operating principles and rules
   - Current state snapshot
   - Changelog section
2. **Updated `.gitignore`** — Added `docs/TRACKING.md` to exclusion list
3. **Updated `docs/DOCUMENTATION.md`** — Refreshed to match current project state (v2.1):
   - Updated file line counts to actual current values
   - Updated frontend component tree (added SystemHealthCard, OscillatorPane)
   - Updated Flink TaskManager memory config to match docker-compose (7168m cap)
   - Updated version info and last-updated date
   - Added note about HTTPS automation (certbot, DuckDNS)

### 2026-04-25 — Session 2: Full Project Refactoring

**Task:** Comprehensive project refactoring across 8 batches

**Changes:**
1. **Batch 1: Project Structure** — Migrated `serving/` → `backend/` (MVC: api/, services/, models/, core/). Updated Dockerfile and docker-compose.yml. Deleted debug artifacts, dead candle-aggregator block, old test files.
2. **Batch 2: Backend MVC** — Created `core/constants.py` (DRY), Pydantic models (candle, ticker, health), `services/candle_service.py` (280 lines of shared logic). Replaced urllib with httpx. Extracted health endpoint.
3. **Batch 3: Frontend Cleanup** — Removed all tech-stack references (Iceberg, Trino, FastAPI, Nginx, Flink) from UI text and comments. Sanitized SystemHealthCard dependency names.
4. **Batch 4: Dev/Prod Switching** — Created docker-compose.override.yml (dev), docker-compose.prod.yml (prod), Makefile with 11 targets.
5. **Batch 5: Docker Optimization** — Added deploy.resources.limits.memory to all 14 services. Pinned Python dependencies.
6. **Batch 6: Testing** — Created pytest framework (pyproject.toml, conftest.py, 5 test directories). Wrote 40 tests (20 unit, 9 model, 9 security + 2 extras). Fixed Python 3.9 compatibility.
7. **Batch 7: Security** — Added nginx rate limiting (30r/s API, 5r/s WS), security headers (HSTS, X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, Referrer-Policy), request size limits.
8. **Batch 8: Documentation** — Updated DOCUMENTATION.md and TRACKING.md to reflect new architecture.

**Notes/Gotchas discovered:**
- Python 3.9 on macOS doesn't support `X | None` syntax — needs `from __future__ import annotations` for runtime, but Pydantic v2 evaluates annotations at class definition time, so `Optional[float]` is required for Pydantic models specifically.
- `from __future__ import annotations` works fine for all non-Pydantic files (FastAPI Query params still work because FastAPI evaluates them differently).
- docker-compose.yml `mem_limit` is deprecated — use `deploy.resources.limits.memory` instead.

**Impact:**
- Backend: complete restructure (22 Python files)
- Frontend: comment/text cleanup (4 files)
- Infrastructure: 5 new files (Makefile, docker-compose.override.yml, docker-compose.prod.yml, pyproject.toml, requirements-test.txt)
- Testing: 40 passing tests across 3 test files
- Docs: updated 2 files

---

<!-- TEMPLATE FOR FUTURE ENTRIES:

### YYYY-MM-DD — Session N: [Title]

**Task:** [What was requested]

**Changes:**
1. **[File/Component]** — [What changed and why]
2. ...

**Notes/Gotchas discovered:**
- [Any new patterns, bugs, or things to remember]

**Impact:**
- [Which layers were affected: backend/frontend/infra/docs]

-->
- **April 25, 2026**: Merged `SYSTEM_ARCHITECTURE.md` into `DOCUMENTATION.md`. Created a new user-friendly `README.md` with badges and quick start guide.
