# Architecture & Phase Plan

Local-first crypto trading **intelligence** platform for research and education.
No live trading, no order execution, no exchange-account integration, no paid services.

Status: **Phase 1 and 1.1 implemented** (data foundation and source independence).
Later phases are proposals and are not built until approved.

## Core engineering rule: the strategy engine never knows where data came from

```
MarketDataProvider ── OHLCVSource / FundingSource / OpenInterestSource
   ├── BinanceProvider     (OHLCV + funding + OI)      primary
   ├── CoinbaseProvider    (OHLCV, spot, USD book)     secondary / cross-check
   ├── CoinGeckoProvider   (OHLCV, index only)
   └── FutureProvider      (implements only the capabilities it has)
          │  ingestion: fetch → raw → validate → canonical Parquet
          ▼
   MarketDataReader (domain/ports.py)   ← the ONLY data interface for features/regime/strategy/scanner/risk
          • one provider per series, chosen by CIP_SOURCE_PRIORITY (never mixed)
          • frames carry no provider / ingested_at columns
```

Enforced by `tests/test_architecture.py`, which inspects imports with `ast`. Swapping or adding a
provider changes ingestion and configuration only, never analytics.

---

## Phase plan

| Phase | Scope | MVP items |
|---|---|---|
| **1 ✅** | Provider interface, Binance public adapter, raw landing, validation, processed Parquet, DuckDB query layer, SQLite catalog, CLI, read-only API | 1, 2, 3 |
| **1.1 ✅** | Capability-based provider interfaces, CoinGecko adapter (2nd source), `MarketDataReader` port with one source per series, OHLCV-first default, architecture test | 1 |
| **1.1b ✅** | CoinbaseProvider as secondary OHLCV source (replaces CoinGecko in that role; CoinGecko kept, lowest priority) | 1 |
| 1.2 | Derivatives: Binance Vision bulk backfill (OI 5-min history since 2020-09, long/short ratios, funding) + liquidations WebSocket collector (Binance `!forceOrder@arr`; history only from first run) | — |
| 2 | Feature engineering (transparent indicators) + glossary knowledge base (YAML) | 4, 8 (data) |
| 3 | Market-regime detection + deterministic strategy engine + opportunity scanner + explainable signal objects | 5, 6, 7 |
| 4 | Streamlit dashboard: market overview, symbol detail, scanner, clickable glossary, data health | 9, 8 (UI) |
| 5 | Paper-trading journal (SQLite) + basic risk engine (position sizing, R-multiples) | 10 |
| 6 | Local AI analyst: Ollama + local embeddings + vector store over glossary and signals (explain only) | — |
| 7 | Backtesting of deterministic strategies + journal review loop | — |

---

## A. Repository structure

```
crypto-intel/
├── pyproject.toml            # deps, CLI entry point, pytest config
├── .env.example              # all settings; .env is git-ignored
├── README.md
├── docs/ARCHITECTURE.md      # this file
├── src/cryptointel/
│   ├── config.py             # pydantic-settings, CIP_* env vars
│   ├── retry.py              # backoff decorator (adapted from stock-index-app)
│   ├── domain/
│   │   ├── models.py         # Market (spot/perp/index), Dataset, Interval, canonical symbols
│   │   ├── schemas.py        # canonical table schemas + primary keys
│   │   └── ports.py          # MarketDataReader: the only data port for analytics
│   ├── providers/
│   │   ├── base.py           # MarketDataProvider + capability ABCs, RawPage, FetchResult
│   │   ├── http.py           # shared JSON GET: retry, 429 handling, raw capture
│   │   ├── binance.py        # Binance public REST adapter
│   │   ├── coinbase.py       # Coinbase Exchange public adapter (OHLCV, spot)
│   │   ├── coingecko.py      # CoinGecko public adapter (OHLCV, index)
│   │   └── registry.py       # build provider by name from settings
│   ├── storage/
│   │   ├── raw.py            # immutable gzipped raw payloads
│   │   ├── parquet_store.py  # processed series + rejected records
│   │   ├── catalog.py        # SQLite lineage + DQ history
│   │   ├── query.py          # DuckDB read repository (provider-aware)
│   │   └── reader.py         # SourceResolvingReader implements MarketDataReader
│   ├── validation/
│   │   ├── validator.py      # configurable DQ checks; validated/rejected split
│   │   ├── rules.py + dq_rules.toml  # rule config (enabled, severity, thresholds)
│   │   ├── report.py         # DQ report rows
│   │   └── audit.py          # full-history audit over the store
│   ├── ingestion/pipeline.py # fetch → raw → validate → store → catalog
│   ├── api/main.py           # FastAPI (read-only)
│   └── cli.py                # cip ingest | validate | summary | serve
│   # later phases add: features/, regime/, strategy/, scanner/, knowledge/,
│   # risk/, journal/, ai/, backtest/, dashboard/
├── tests/                    # one test module per component + live contract tests
└── data/                     # git-ignored; recreated by `cip ingest`
```

## B. Dependencies

All free / open source. Each is justified; nothing optional is installed.

| Package | License | Why it's needed | Phase |
|---|---|---|---|
| httpx | BSD-3 | HTTP client; built-in `MockTransport` means no extra mocking library | 1 |
| pandas, numpy | BSD-3 | dataframes, numerics | 1 |
| pyarrow | Apache-2.0 | Parquet read/write | 1 |
| duckdb | MIT | SQL over Parquet, no server | 1 |
| pydantic, pydantic-settings | MIT | typed config from `.env` | 1 |
| fastapi, uvicorn | MIT / BSD-3 | local read-only API | 1 |
| pytest | MIT | tests | 1 |
| *(proposed)* pyyaml | MIT | glossary files | 2 |
| *(proposed)* streamlit, plotly | Apache-2.0 / MIT | dashboard, candlestick charts | 4 |
| *(proposed)* ollama (client) | MIT | talk to local Ollama server | 6 |
| *(proposed)* chromadb **or** faiss-cpu | Apache-2.0 / MIT | local vector store (decide in Phase 6 — the corpus is small enough that plain numpy cosine may suffice) | 6 |

Deliberately **not** used: TA-Lib (C dependency, opaque), pandas-ta (maintenance
uncertainty). Indicators will be implemented in transparent, tested pandas code.
`scipy` will be added only if a specific calculation needs it.

## C. Data providers

Verified reachable from this machine on 2026-10-19 (no keys needed): Binance spot
and USD-M futures, Binance.US, Bybit, OKX, Coinbase Exchange, Kraken (spot and
futures), Deribit, Hyperliquid.

MVP universe: **BTC-USDT, ETH-USDT, SOL-USDT**. The MVP depends on **one** provider
(Binance). OHLCV is ingested by default; derivatives are opt-in.

| Provider | Use | Notes |
|---|---|---|
| **Binance (primary)** | OHLCV spot + perp, taker-buy volume; opt-in funding and OI | Deepest free history. OI history is limited to **~30 days** by the API, so OI accumulates locally from when you start ingesting. |
| **Coinbase Exchange (implemented, secondary)** | Independent spot OHLCV with volume, cross-checks, fallback | Keyless. ≤300 candles/call, newest first, `[time, low, high, open, close, volume]`. No 4h granularity (aggregated from 1h, complete buckets only). USD book used for USD-stablecoin symbols. Venue outages appear as gaps (e.g. 10 h on 2025-10-25). |
| **Binance Vision archive (planned 1.2)** | Bulk history: OI at 5-min since 2020-09, long/short and taker ratios, funding, klines (with checksums) | Same provider as primary, so no new dependency. Removes the 30-day OI limit of the REST API. No liquidation files. |
| **CoinGecko (implemented, lowest priority)** | USD index OHLC | Keyless public tier: a few calls/min, `days` must be one of a fixed set, 4h candles for ≤30 days, candle timestamps are close times, **no volume**. Stored as `market=index`. |
| Liquidations (planned 1.2) | Long squeezes, liquidation cascades | Binance removed historical REST (`allForceOrders`); free data is **WebSocket only** (`!forceOrder@arr`, which sends one sample per symbol per second). Needs an always-on collector. Paid aggregators (e.g. CoinGlass) are excluded. |
| Bybit / OKX (candidates) | Second source for cross-checks; longer OI history | Same interface, new adapter only |
| Coinbase / Kraken | US-regulated spot reference prices | For cross-venue outlier verification |
| Deribit | Options and basis data | Later |

Every provider implements `MarketDataProvider` and maps its data into the canonical
schemas, so nothing downstream depends on a specific exchange. Before relying on a
provider long term, check its terms of use for your jurisdiction.

## D. Local storage design

```
data/
├── raw/{provider}/{dataset}/{market}/{symbol}/{interval|all}/{YYYY-MM-DD}/{run_id}.json.gz
├── processed/{dataset}/provider=…/market=…/symbol=…/[interval=…/]data.parquet
├── rejected/{dataset}/{run_id}.parquet        # rejected rows + dq_reason + run_id
├── reports/dq_report_<UTC>.csv                # full-history audit reports
└── catalog.sqlite                             # lineage + DQ report
```

- **Raw (bronze):** immutable, exactly as received, for replay and auditing.
- **Processed (silver):** canonical schema, validated rows only, one file per
  series, rewritten atomically. Hive-style paths keep the layout compatible with a
  later move to Delta Lake or Databricks.
- **Rejected:** nothing is silently dropped. Every row that didn't reach `processed/`
  is kept here with its reason, including superseded duplicates and still-open candles.
- **Features (gold, Phase 2):** `features/` Parquet derived only from processed data.
- **DuckDB** is used in-memory for SQL over Parquet. No database server, and no
  database file to keep in sync.
- **SQLite** holds relational metadata: runs, DQ issues, and later signals and the journal.

## E. Database schema

**Implemented (SQLite `catalog.sqlite`):**

- `ingestion_runs(run_id PK, provider, dataset, market, symbol, interval, requested_start, effective_start, requested_end, status, rows_fetched, rows_written, rows_rejected, rows_dropped, quality_score, raw_path, error, started_at, finished_at)`
- `dq_report(id PK, run_timestamp, run_id, scope[ingest|audit], dataset, provider, market, symbol, timeframe, check_name, severity, status[pass|info|warn|fail], failed_rows, total_rows, message, sample_json)`: one row per series × check, passes included
- Catalogs from before Phase 1B are migrated automatically (`rows_quarantined` → `rows_rejected`). The legacy `data_quality_issues` table is left untouched as history.

**Canonical Parquet schemas (implemented):**

- `ohlcv`: ts, provider, market, symbol, interval, open, high, low, close, volume, quote_volume, trade_count, taker_buy_volume, taker_buy_quote_volume, ingested_at. PK = (provider, market, symbol, interval, ts)
- `funding`: ts, provider, market, symbol, funding_rate, mark_price, ingested_at
- `open_interest`: ts, provider, market, symbol, interval, open_interest, open_interest_value, ingested_at

**Proposed (later phases, not created):**

- `signals(signal_id PK, created_at, symbol, market, interval, strategy_id, strategy_version, direction, score, regime, evidence_json, triggered_rules_json, invalidation_json, risk_json, data_snapshot_ts, data_hash)`: Phase 3
- `journal_trades(trade_id PK, signal_id FK NULL, symbol, side, entry_ts, entry_price, size, stop, target, exit_ts, exit_price, fees, r_multiple, thesis, emotions, lessons, tags)`: Phase 5
- `journal_notes(id PK, trade_id FK, ts, note)`: Phase 5

## F. API design

Read-only. There are no trading endpoints, and a test enforces that.

| Endpoint | Phase |
|---|---|
| `GET /health` | 1 ✅ |
| `GET /api/v1/datasets` (stored series, row counts, ranges) | 1 ✅ |
| `GET /api/v1/candles?symbol&interval&market&start&end&limit` | 1 ✅ |
| `GET /api/v1/funding?symbol&start&end&limit` | 1 ✅ |
| `GET /api/v1/open-interest?symbol&interval&start&end&limit` | 1 ✅ |
| `GET /api/v1/ingestion-runs` | 1 ✅ |
| `GET /api/v1/data-quality?run_id&scope&symbol&min_severity` | 1 ✅ |
| `GET /api/v1/features`, `/regime` | 2–3 |
| `GET /api/v1/scanner`, `/signals/{id}` | 3 |
| `GET /api/v1/glossary`, `/glossary/{term}` | 2 |
| `GET/POST /api/v1/journal/...` (paper trades only) | 5 |
| `POST /api/v1/ai/explain` (explains an existing signal or term) | 6 |

Ingestion is CLI-only for now (`cip ingest`), which keeps write paths out of the API.

## G. UI structure (Phase 4, Streamlit)

1. **Market Overview:** universe table with price, change, regime, funding and OI change.
2. **Symbol Detail:** candlestick chart, indicators, regime band, funding/OI panels, signal cards.
3. **Opportunity Scanner:** ranked setups. Each card shows evidence, triggered rules,
   score, invalidation, risks and timestamp, with a "why?" expander.
4. **Glossary:** searchable. Every term elsewhere in the UI links here (popover + page).
5. **Paper Journal:** log trades against signals, R-multiples, notes, review.
6. **Data Health:** ingestion runs, DQ issues, freshness per series (uses Phase 1 catalog).

## H. Strategy-engine architecture (Phase 3)

```
processed data → features (pure functions, tested) → regime classifier (rules)
   → Rules: (features, params) → RuleResult{id, triggered, evidence{metric: value, threshold}}
   → Strategy = rule set + regime filter + scoring weights + invalidation + risk template (versioned, params in YAML)
   → Scanner runs strategies over the universe → Signal objects (schema in §E)
```

- Fully deterministic: the same data and parameters always produce the same signal.
  Each signal records its data snapshot timestamp/hash and strategy version.
- Each rule's evidence names the glossary terms it uses, so the UI can link them.
- Score = weighted share of satisfied rules, shown as a breakdown and never as a
  single opaque number.
- Language is "setup observed", not "buy". No sizing or leverage automation.

## I. AI / RAG architecture (Phase 6)

- `LLMProvider` interface with an `OllamaProvider` and a `TemplateProvider` fallback,
  so the app runs fully without any LLM.
- Corpus: glossary YAML, strategy docs, and your own journal notes, chunked and
  embedded locally (`nomic-embed-text` via Ollama, which avoids pulling PyTorch).
- The LLM only sees deterministic outputs (signal objects, glossary entries). The
  prompt forbids introducing new numbers, and responses must cite evidence fields.
  It explains and educates; it never generates signals.
- **8 GB RAM note:** use 3–4B quantized models (e.g. `llama3.2:3b`, `qwen2.5:3b`,
  about 2–2.5 GB). 7–8B models (about 5 GB) will compete with everything else on this Mac.

## J. Testing strategy

- **Unit:** each module in isolation (domain, provider, validator, stores, catalog, repository).
- **Provider:** `httpx.MockTransport` simulates pagination, rate limits (429), 5xx
  retries, fail-fast 4xx/418/451, malformed numbers, and the OI "latest rows" quirk.
- **Pipeline:** a fake provider tests end-to-end ingestion, incremental resume,
  up-to-date short-circuit, failure recording, and raw/processed/rejected separation.
- **API:** FastAPI `TestClient` against a temp data dir, including a "no trading endpoints" test.
- **Live contract tests** (`pytest -m live`, deselected by default): confirm Binance's
  real payloads still match our mapping.
- Later phases: golden-file tests for indicators against hand-computed values, and
  property tests for signal determinism.

## K. Estimated local resource usage (M2, 8 GB RAM)

| Item | Phase 1 measured | Later estimate |
|---|---|---|
| Disk: data (3 symbols × spot/perp × 1h/4h/1d since 2024 + funding + OI) | **27 MB** (13 raw + 14 processed; ~50 MB after adding Coinbase + CoinGecko) | ~10 MB/symbol/year incl. raw; 15m interval ≈ 4× |
| Disk: virtualenv | ~350 MB | +300 MB with Streamlit/Plotly |
| RAM: ingestion / API | < 300 MB | Streamlit +150–250 MB |
| RAM: local LLM (Phase 6) | — | 2–2.5 GB (3B Q4) |
| Disk: LLM models | — | 2–5 GB |
| Full initial backfill | **~90 s**, ~200 requests (well under Binance limits) | incremental runs: seconds |
| Ongoing cost | **$0** | $0 |
