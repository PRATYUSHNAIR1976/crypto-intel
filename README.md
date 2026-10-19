# Crypto Intel

A local-first crypto trading **intelligence** platform for research and education.
It runs entirely on your machine using free public market data. It does **not**
place trades, connect to exchange accounts or call paid APIs.

> Status: **Phase 1, the data foundation.** Ingestion, raw storage, data-quality
> validation, normalized Parquet storage, a DuckDB query layer, and a read-only API.
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
cip ingest --provider coingecko              # USD index, 4h, last 30 days (cross-check source)
cip ingest --symbols BTC-USDT --markets perp --intervals 15m --since 2025-01-01
cip ingest --full-refresh                    # re-fetch from --since; upsert dedupes
```

## Providers and source independence

```
MarketDataProvider          identity + supports()
  ├── OHLCVSource            fetch_ohlcv
  ├── FundingSource          fetch_funding_rates
  └── OpenInterestSource     fetch_open_interest

BinanceProvider   = OHLCV + Funding + OpenInterest   (spot, perp)
CoinGeckoProvider = OHLCV                            (index, 4h, no volume)
```

Everything downstream of storage reads through one port, `MarketDataReader`. It
picks **one** provider per series (`CIP_SOURCE_PRIORITY`) and returns frames
**without** `provider`/`ingested_at` columns, so analytics and strategy code can't
know where data came from. `tests/test_architecture.py` fails the build if
strategy-side packages import providers, storage, ingestion, HTTP or database clients.

## Pipeline

```
Binance public REST ──▶ raw/ (gzipped JSON, immutable)
                     └▶ validate ──▶ normalized/ (Parquet, canonical schema)
                                 └▶ quarantine/ (failed rows + reason)
                     every run + issue ──▶ catalog.sqlite
normalized/ ──DuckDB──▶ FastAPI (read-only)
```

**Data-quality checks.** Errors are quarantined. Warnings are kept and reported.
Gaps are never filled.

| Check | Severity |
|---|---|
| null/unparseable OHLC or timestamp, future or misaligned timestamp | error |
| volume not provided by source (e.g. index data) | warning |
| price ≤ 0, negative volume, high/low not bounding open/close, taker volume > volume | error |
| negative open interest | error |
| duplicate keys (last wins), missing intervals, zero volume, extreme funding, price outliers (robust z > 12) | warning |
| still-open candle | info (dropped) |

## Tests

```bash
.venv/bin/pytest              # offline suite (no network)
.venv/bin/pytest -m live      # contract tests against the real Binance API
```

## Data sources and limits

- **Binance** spot and USD-M perpetual public endpoints, no key required.
  Open-interest history is limited to **~30 days**. Run `cip ingest` regularly and
  the local history grows from there.
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
