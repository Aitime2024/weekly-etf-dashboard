# Weekly Pay ETF Dashboard

A GitHub Pages dashboard + daily GitHub Action job that scrapes WeeklyPayers and writes JSON files used by the UI.

## Data source
- https://weeklypayers.com/ (table: tickers, issuer, current price, last dividend)
- https://weeklypayers.com/calendar/ (calendar: ex/record vs payment dates)

## Output files
- `data/weekly_etfs.json` (primary file the UI reads)
- `data/items.json` (backup / fallback)
- `data/alerts.json` (alert list)
- `data/history/YYYY-MM-DD.json` (daily snapshots for comparisons)

## Run locally
```bash
pip install -r requirements.txt
python scraper.py
python -m http.server 8000
