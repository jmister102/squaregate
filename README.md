# SquareGate Bloomberg Analyzer

GUI tool for Bloomberg-based equity screening and Excel report generation.

## What it does

1. Loads a list of Bloomberg equity tickers from a CSV file
2. Pulls reference, financial, and float data via the Bloomberg API
3. Applies four screening filters (ADV, float market cap, cash runway, burn rate)
4. Exports four dated Excel workbooks per run:

| File | Contents |
|------|----------|
| `filter_results_YYYYMMDD_HHMMSS.xlsx` | All tickers with pass/fail per test |
| `passing_tickers_YYYYMMDD_HHMMSS.xlsx` | Tickers that passed every filter |
| `spreadsheet_data_YYYYMMDD_HHMMSS.xlsx` | Full data in original column order |
| `bloomberg_raw_YYYYMMDD_HHMMSS.xlsx` | Complete raw Bloomberg dump |

## Requirements

- **Bloomberg Terminal** must be running on the same machine (API listens on port 8194)
- Python 3.11+
- See `requirements.txt`

```bash
pip install -r requirements.txt
```

> **Note on `blpapi`:** The package is on PyPI (`pip install blpapi`) but requires
> Bloomberg's C++ runtime libraries, which are installed with the Bloomberg Terminal.

## Ticker file format

One Bloomberg security identifier per line — no headers, no extra columns:

```
AAPL US Equity
MSFT US Equity
NVDA US Equity
```

See `sample_tickers.csv` for a working example.

## Running from source

```bash
python main.py
```

The GUI will show a **red/green Bloomberg status indicator** that updates every 5 seconds.
The **Run Analysis** button is enabled only when both a valid ticker file is loaded *and*
Bloomberg is detected as running.

## Building a standalone .exe (Windows)

```bat
build.bat
```

This runs PyInstaller and produces `dist\SquareGate.exe`.

> If the target machine doesn't have Bloomberg Terminal installed, copy the
> `blpapi*.dll` files from your Python `site-packages\blpapi` folder into
> the same directory as `SquareGate.exe`.

## Project structure

```
squaregate/
├── main.py                  # GUI + analysis orchestration
├── bloomberg_collector.py   # Bloomberg API wrapper (BDP / BDH)
├── filters.py               # Screening filter logic
├── excel_formatter.py       # Openpyxl number/date formatting
├── requirements.txt
├── build.bat                # PyInstaller build script
└── sample_tickers.csv       # Example input file
```

## Screening filters

| Test | PASS condition |
|------|----------------|
| `test_float_lt_75M` | Float market cap (last 60 days) < $75 M |
| `test_adv_gt_50k` | 100-day **or** 20-day ADV > $50 k |
| `test_lt_24m_runway` | Cash runway < 24 months (quarterly or TTM) |
| `test_adv_div_dailyburn_lt_35pct` | Cash-flow positive **or** ADV / daily burn < 35% |
