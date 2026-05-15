# BTCUSDT Market Data Collector & Live Viewer

Real-time Binance Spot market-data infrastructure for **BTCUSDT**.

This repository contains two complementary components:

1. **Python collector** — subscribes to Binance WebSocket streams, normalizes market-data events, stores them as partitioned Parquet datasets, and reconstructs a synchronized local order book.
2. **Standalone live viewer** — opens directly in a browser and displays a live BTCUSDT chart, market snapshot, recent trades, and top-10 order book levels.

The project is designed for market-data research, microstructure analysis, feature engineering, replay experiments, and prototyping trading ideas. It does **not** place orders and does **not** provide trading advice.

---

## Features

- Collects live BTCUSDT Spot data from Binance WebSocket streams.
- Supports:
  - raw trades: `btcusdt@trade`
  - aggregated trades: `btcusdt@aggTrade`
  - best bid/ask updates: `btcusdt@bookTicker`
  - partial depth snapshots: `btcusdt@depth20@100ms`
  - diff depth updates: `btcusdt@depth@100ms`
- Requests microsecond timestamps via Binance WebSocket query parameters.
- Writes normalized records to partitioned Parquet datasets.
- Reconstructs a local order book using REST depth snapshot + diff depth updates.
- Emits periodic local order book snapshots to Parquet.
- Includes a browser-only live viewer powered by Lightweight Charts.
- Builds 1-second candles locally from raw trades.
- Shows last price, best bid, best ask, spread, 1-second volume, recent trades, and top-10 order book.

---

## Repository structure

```text
btcusdt-market-data-collector/
  main.py                         # Async Python collector -> Parquet
  btcusdt_live_viewer.html        # Standalone browser live viewer
  requirements.txt                # Runtime Python dependencies
  pyproject.toml                  # Project metadata
  .gitignore                      # Keeps generated data out of Git
  LICENSE                         # Repository license notice
  docs/
    data-schema.md                # Dataset and field overview
    site-project-entry.ru.md      # Russian project-card text for personal website
  scripts/
    run_collector.sh              # Convenience script for Linux/macOS
    open_viewer.sh                # Convenience script for Linux/macOS
  data/
    .gitkeep                      # Placeholder; generated data is ignored
  assets/
    .gitkeep                      # Place screenshots / cover image here
  .github/workflows/
    python-syntax-check.yml       # Basic CI syntax check
```

---

## Installation

Use Python 3.10+.

```bash
git clone https://github.com/YOUR_USERNAME/btcusdt-market-data-collector.git
cd btcusdt-market-data-collector

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Run the collector

```bash
python main.py --data-dir ./data
```

Optional parameters:

```bash
python main.py \
  --data-dir ./data \
  --flush-seconds 2 \
  --max-records-per-stream 10000 \
  --book-snapshot-seconds 1 \
  --book-snapshot-levels 50 \
  --log-level INFO
```

Useful arguments:

| Argument | Default | Description |
|---|---:|---|
| `--data-dir` | `./data` | Root directory for Parquet datasets. |
| `--flush-seconds` | `2` | Periodic Parquet flush interval. |
| `--max-records-per-stream` | `10000` | Flush a stream buffer when it reaches this many records. |
| `--book-snapshot-seconds` | `1` | Frequency for local order book snapshot emission. |
| `--book-snapshot-levels` | `50` | Number of top bid/ask levels saved in each local book snapshot. |
| `--max-session-seconds` | `86100` | Proactively rotates WebSocket session before the Binance 24h limit. Use `0` to disable. |
| `--log-level` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |

---

## Run the live viewer

Open the HTML file directly in a browser:

```bash
open btcusdt_live_viewer.html
```

On Windows, double-click the file or run:

```powershell
start btcusdt_live_viewer.html
```

The viewer connects directly to Binance WebSocket streams from the browser. It does not require the Python collector to be running.

---

## Data layout

The collector writes partitioned Parquet datasets under the selected `data` directory:

```text
data/
  stream=trade/date=YYYY-MM-DD/hour=HH/
  stream=aggTrade/date=YYYY-MM-DD/hour=HH/
  stream=bookTicker/date=YYYY-MM-DD/hour=HH/
  stream=partialDepth/date=YYYY-MM-DD/hour=HH/
  stream=diffDepth/date=YYYY-MM-DD/hour=HH/
  stream=localBook/date=YYYY-MM-DD/hour=HH/
```

This layout is convenient for later research workflows such as replay, aggregation, feature engineering, signal development, and backtesting experiments.

Generated Parquet files can become large quickly. They are intentionally excluded from Git by `.gitignore`.

---

## Local order book reconstruction

The collector maintains a local order book using the standard snapshot + diff update approach:

1. Buffer incoming diff depth events.
2. Request a REST depth snapshot.
3. Compare snapshot `lastUpdateId` with buffered update IDs.
4. Discard already-covered events.
5. Apply remaining sequential diff updates.
6. Resynchronize if an update gap is detected.

The resulting top bid/ask levels are periodically written as the `localBook` dataset.

---

## Example research use cases

- Market microstructure analysis.
- Spread and liquidity monitoring.
- Order book imbalance features.
- Trade aggressor flow analysis.
- Intraday volatility and volume studies.
- Dataset creation for replay/backtesting.
- Prototype infrastructure for crypto market-data pipelines.

---

## Notes

- The repository stores source code and documentation only.
- Large generated datasets should be stored outside GitHub or uploaded separately as small samples.
- The live viewer uses a CDN version of Lightweight Charts.
- This project is for educational and research purposes only.
