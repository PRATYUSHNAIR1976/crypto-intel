# Crypto Intel

A local-first crypto trading **intelligence** platform for research and education.
It runs entirely on your machine using free public market data. It does **not**
place trades, connect to exchange accounts or call paid APIs.

> Status: **Phase 1, the data foundation.** Ingestion, raw storage, data-quality
> configurable data-quality validation, processed Parquet storage, a DuckDB query layer, and a read-only API.
> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design and phase plan.

## Quick start

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env              # no API keys needed for Phase 1

.venv/bin/cip ingest              # backfill (~90 s first time, incremental afterwards)
.venv/bin/cip validate            # full-history data-quality audit
.venv/bin/cip summary             # what's stored
.venv/bin/cip serve               # API on http://127.0.0.1:8000  (docs at /docs)
```

Useful ingest options:

```bash
cip ingest                                   # OHLCV from Binance (default)
cip ingest --datasets funding,open_interest  # derivatives, opt-in
cip ingest --provider coinbase               # independent spot OHLCV with volume (since 2024: ~4.5 min)
cip ingest --provider coingecko              # USD index, 4h, last 30 days
cip ingest --symbols BTC-USDT --markets perp --intervals 15m --since 2025-01-01
cip ingest --full-refresh                    # re-fetch from --since; upsert dedupes
```

## Providers and source independence

```
MarketDataProvider          identity + supports()
  ├── OHLCVSource            fetch_ohlcv
  ├── FundingSource          fetch_funding_rates
  └── OpenInterestSource     fetch_open_interest

BinanceProvider   = OHLCV + Funding + OpenInterest   (spot, perp)       primary
CoinbaseProvider  = OHLCV                            (spot, USD book)   secondary / cross-check
CoinGeckoProvider = OHLCV                            (index, 4h, no volume)
```

Everything downstream of storage reads through one port, `MarketDataReader`. It
picks **one** provider per series (`CIP_SOURCE_PRIORITY`) and returns frames
**without** `provider`/`ingested_at` columns, so analytics and strategy code can't
know where data came from. `tests/test_architecture.py` fails the build if
strategy-side packages import providers, storage, ingestion, HTTP or database clients.

## Pipeline

```
provider REST ──▶ data/raw/        gzipped JSON, exactly as received (immutable)
              └▶ validate ──▶ data/processed/  validated rows, canonical Parquet
                          ├─▶ data/rejected/   rejected rows + dq_reason (never silently dropped)
                          └─▶ DQ report        catalog.sqlite (dq_report) + data/reports/*.csv
data/processed/ ──DuckDB──▶ MarketDataReader ──▶ FastAPI (read-only)
```

Logs are structured JSON on stderr (`CIP_LOG_FORMAT=text` gives key=value lines).

## Data quality

Deterministic checks, configured in
[`validation/dq_rules.toml`](src/cryptointel/validation/dq_rules.toml). Override any
subset with your own TOML via `CIP_DQ_RULES_PATH`; typos in check names are rejected.
Severity `error` rejects the row, `warning` keeps it and reports it. Nothing is ever
repaired, and gaps are never filled.

| Check | Default | Applies to |
|---|---|---|
| `null_values` | error | all |
| `duplicate_timestamps`: earlier copies go to rejected, last kept | warning | all |
| `future_timestamp`, `misaligned_timestamp` | error | all / gridded |
| `high_below_low`, `high_below_open`, `high_below_close`, `low_above_open`, `low_above_close` | error | OHLCV |
| `negative_price`, `zero_price` | error | OHLCV (+ funding mark price) |
| `negative_volume`, `taker_volume_exceeds_volume` | error | OHLCV, open interest |
| `extreme_price_jump`: \|close-to-close\| > 10% / 15% / 25% / 40% for 15m / 1h / 4h / 1d | warning | OHLCV |
| `zero_volume`, `missing_volume`, `extreme_funding_rate` | warning | OHLCV / funding |
| `missing_timestamps`: every missing candle | warning | gridded series |
| `timestamp_gaps`: runs longer than `max_gap_intervals` (1) | warning | gridded series |
| `incomplete_candle`: still open, excluded until it closes | info | OHLCV |

**DQ report:** one row per series × check, passes included. Columns: `run_timestamp, run_id,
scope, dataset, provider, market, symbol, timeframe, check_name, severity, status
(pass|info|warn|fail), failed_rows, total_rows, message, sample`. `cip validate` audits all
stored history and writes `data/reports/dq_report_<UTC>.csv`. Ingestion records a report
for every batch. `GET /api/v1/data-quality` serves it.

## Tests

```bash
.venv/bin/pytest              # offline suite (no network)
.venv/bin/pytest -m live      # contract tests against the real Binance API
```

## Data sources and limits

- **Binance** spot and USD-M perpetual public endpoints, no key required.
  Open-interest history is limited to **~30 days**. Run `cip ingest` regularly and
  the local history grows from there.
- **Coinbase Exchange** public API, no key required. Spot candles with real volume and
  years of history. USDT/USDC symbols read the USD book, because Coinbase's USDT books
  are thin. 4h candles are built from 1h candles, since Coinbase has no 4h granularity.
  Max 300 candles per call. Against Binance 2024–2026: median close difference −0.01%,
  99th percentile 0.19%.
- **CoinGecko** public API, no key required (a free Demo key raises the rate limit).
  It provides a cross-exchange USD index (USDT/USDC treated as USD), 4h candles for the
  last 30 days, and no volume. It's stored as `market=index`, for cross-checks only.
  The keyless tier allows only a few calls per minute.
- **Liquidations** are not yet supported. Free sources are real-time WebSocket only
  (no historical REST), so they need an always-on collector (planned).
- Check each provider's terms of use for your jurisdiction.

## Principles

Deterministic logic first, AI explanation second. Every future signal will carry its
evidence, triggered rules, score, invalidation conditions, risks and timestamp.
Secrets live only in `.env`, which is git-ignored.
