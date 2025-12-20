# Backend Data Extraction & Storage

This document summarizes what the backend extracts, produces, and stores based on the current Flask service implementation. It focuses on data sources, processing flows, MongoDB collections, and generated files.

## Overview
- Service: Flask app that consolidates NSE market-cap and PR (net traded value) CSVs, caches raw files, computes per-symbol metrics, and produces Excel exports.
- Primary code: Backend/app.py (NSE downloads, consolidation, dashboard, Google Drive), consolidate_marketcap.py (CSV -> consolidated DataFrame/Excel), nse_symbol_metrics.py (per-symbol metrics via NSE NextApi).
- Persistence: MongoDB (`mongo_URI`, DB `Stocks`) with multiple collections plus optional Google Drive storage for Excel outputs.

## Data Sources
- NSE Bhavcopy archive ZIP (`CM - Bhavcopy (PR.zip)`) fetched per date or date-range; contains `mcap*.csv` (market cap) and `pr*.csv` (net traded value). Fallback to Bhavcopy CSV when MCAP missing.
- User-uploaded CSVs (MCAP-style) for ad-hoc consolidation.
- NSE NextApi `GetQuoteApi` for per-symbol metrics (impact cost, FFMC, market cap, turnover, price, indices).
- Google Drive (optional) for storing generated Excel files.

## Processing Flows (key endpoints)
- `/api/preview`: Upload CSVs -> consolidate preview (first rows, column list, dates) without writing to DB.
- `/api/consolidate`: Upload CSVs -> consolidate -> Excel (`Finished_Product.xlsx`) -> persist symbol daily values/averages -> return file or upload to Google Drive.
- `/api/download-nse`: Download one trading day ZIP -> cache MCAP/PR CSVs in Mongo -> bulk upsert `symbol_daily` values for that date -> return metadata.
- `/api/download-nse-range`: Same as above for a date range (parallel), caching and upserting per date.
- `/api/consolidate-saved`: Build Excel(s) from cached Mongo CSVs for requested dates (MCAP, PR, or both); optionally persist aggregates/dailies unless `fast_mode` is true; returns ZIP of Excel outputs.
- `/api/nse-symbol-dashboard`: Build per-symbol dashboard via NSE NextApi; persists `symbol_metrics` (enriched with DB primary_index) and can save Excel to Mongo for download.
- `/api/dashboard-data`: Read-only view of top aggregates and latest metrics from Mongo.
- `/api/update-indices` + `/api/download-indices`: Derive primary index per symbol from existing `symbol_metrics`, update documents, and expose a CSV download.
- Excel management: `/api/excel-results` (list), `/api/excel-results/<id>` (download/delete), `/api/excel-results/info/<id>` (metadata).
- Google Drive integration: `/api/google-drive-auth`, `/api/google-drive-files`, `/api/google-drive-status` used by `/api/consolidate` when destination is `google_drive`.

## MongoDB Collections
- `excel_results`: Binary Excel exports with metadata. Fields: `filename`, `file_data` (bin), `file_size`, `created_at`, `file_type`, `metadata` (counts, dates, paging info, etc.).
- `bhavcache`: Cached raw CSVs per date/type. Fields: `date` (YYYY-MM-DD), `type` (`mcap`|`pr`), `file_data` (CSV bytes), `records`, `columns`, `stored_at`, `source`.
- `symbol_daily`: Per-symbol per-date values. Fields: `symbol`, `company_name`, `date` (YYYY-MM-DD), `type` (`mcap`|`pr`), `value`, `source`, `updated_at`. Indexed on `(symbol,type,date)` and `(type,date)`.
- `symbol_aggregates`: Per-symbol averages over a date range. Fields: `symbol`, `company_name`, `type`, `days_with_data`, `average`, `date_range {start,end}`, `source`, `updated_at`. Indexed on `(symbol,type,date_range.start,date_range.end)`.
- `symbol_metrics`: Per-symbol dashboard metrics from NSE NextApi. Fields include `symbol`, `companyName`, `series`, `status`, `index`, `indexList`, `primary_index` (when backfilled), `impact_cost`, `free_float_mcap`, `total_market_cap`, `total_traded_value`, `last_price`, `listingDate`, `basicIndustry`, `applicableMargin`, `as_on`, `source`, `updated_at`.

## Generated Files
- Excel outputs (local or Mongo/Drive):
  - Consolidation: `Finished_Product.xlsx` from uploads; `Market_Cap.xlsx` and `Net_Traded_Value.xlsx` (plus `_Averages.xlsx` variants) from cached data; zipped bundles when both exist.
  - Symbol dashboard: `Symbol_Dashboard_<tag>.xlsx` (created when `save_to_file=true`).
- Downloadable CSV: `indices_<timestamp>.csv` produced by `/api/update-indices`.

## Data Handling Notes
- Caching: `put_cached_csv`/`get_cached_csv` manage raw NSE CSVs in `bhavcache`; `build_consolidated_from_cache` pivots cached CSVs into consolidated DataFrames for Excel and persistence.
- Persistence helpers: `bulk_upsert_symbol_daily_from_df`, `persist_consolidated_results`, `upsert_symbol_metrics`, `upsert_symbol_aggregate`, `upsert_symbol_daily` centralize Mongo writes.
- Corporate actions: `consolidate_marketcap.py` supports optional splits/name changes/delistings via `corporate_actions.json` (auto-template created when missing).
- Concurrency: NSE downloads and symbol dashboard fetches use `ThreadPoolExecutor`; worker counts configurable via request payload (`parallel_workers`, `chunk_size`).
- Limits: Upload size capped at 50MB; MCAP/PR processing trims summary rows like TOTAL/LISTED; pagination in dashboard (`page`, `page_size`, `top_n`).

## Environment
- Required: `mongo_URI` (defaults to `mongodb://localhost:27017/Stocks`).
- Optional: `GOOGLE_CREDENTIALS_PATH` for Drive auth (defaults to `credentials.json`).
- Server: Flask dev server at `:5000` (see Backend/app.py `if __name__ == '__main__'`).

## Quick Reference
- Cache-only flow: `/api/download-nse` or `/api/download-nse-range` -> `bhavcache` + `symbol_daily`.
- Consolidate cached to Excel: `/api/consolidate-saved`.
- Upload-and-consolidate: `/api/consolidate`.
- Symbol metrics dashboard: `/api/nse-symbol-dashboard` -> `symbol_metrics` + optional Excel.
- Aggregate/metrics viewer: `/api/dashboard-data`.
