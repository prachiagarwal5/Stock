# Daily Metrics Storage and Average Calculation

## Overview

This document describes the implementation of daily storage and average calculation for `impact_cost` and `free_float_mcap` values in the Market Cap Consolidation Tool.

## Implementation Summary

### 1. **New Database Collection: `symbol_metrics_daily`**

A new MongoDB collection has been created to store daily values for each symbol:

**Collection Name:** `symbol_metrics_daily`

**Schema:** ⚡ **One document per stock** with date-wise data nested inside
```json
{
  "symbol": "RELIANCE",
  "company_name": "Reliance Industries Limited",
  "created_at": "2026-02-02T10:30:00",
  "last_updated": "2026-02-07T15:45:00",
  "daily_data": [
    {
      "date": "2026-02-02",
      "impact_cost": 0.05,
      "free_float_mcap": 1234567890.50,
      "total_market_cap": 1500000000.00,
      "total_traded_value": 5000000.00,
      "source": "symbol_dashboard",
      "updated_at": "2026-02-02T10:30:00"
    },
    {
      "date": "2026-02-03",
      "impact_cost": 0.04,
      "free_float_mcap": 1245678901.60,
      "total_market_cap": 1510000000.00,
      "total_traded_value": 5200000.00,
      "source": "symbol_dashboard",
      "updated_at": "2026-02-03T11:00:00"
    }
  ]
}
```

**Benefits:**
- ✅ Only 1 document per stock (e.g., 1000 stocks = 1000 documents total)
- ✅ No duplicate data - each date is stored once within the array
- ✅ If data for a date already exists, it gets replaced automatically
- ✅ Easy to query and calculate averages

**Indexes:**
- Unique index on `symbol` for fast lookups
- Index on `daily_data.date` for date range queries

### 2. **Automatic Storage on Dashboard Build**

When the dashboard is built (via `/api/nse-symbol-dashboard`), the system now:

1. **Fetches** current values from NSE API
2. **Stores** the values in `symbol_metrics` collection (as before)
3. **Additionally stores** daily values in `symbol_metrics_daily` collection

**Key Function:** `upsert_symbol_metrics()` in [app.py](../Backend/app.py)

This function has been updated to:
- Store complete symbol metrics in `symbol_metrics` collection
- Extract and store daily metrics (`impact_cost`, `free_float_mcap`, `total_market_cap`, `total_traded_value`) in `symbol_metrics_daily` collection
- **One document per stock**: Each stock has ONE document with a `daily_data` array
- **Date-wise storage**: Each date's data is stored as an object in the `daily_data` array
- **Automatic replacement**: If data for a date already exists, it gets removed and replaced with new values
- **No duplicates**: The `$pull` operation removes old data for the same date before `$push` adds the new data

**How it works:**
```javascript
// Step 1: Ensure document exists for the symbol
update({ symbol: "RELIANCE" }, { 
  $set: { company_name: "...", last_updated: "..." },
  $setOnInsert: { symbol: "RELIANCE", created_at: "..." }
}, { upsert: true })

// Step 2: Remove existing entry for this date (if any)
update({ symbol: "RELIANCE" }, {
  $pull: { daily_data: { date: "2026-02-02" } }
})

// Step 3: Add new entry for this date
update({ symbol: "RELIANCE" }, {
  $push: { daily_data: { date: "2026-02-02", impact_cost: 0.05, ... } }
})
```

### 3. **Average Calculation from Database**

**New Function:** `calculate_averages_from_db(symbols, start_date=None, end_date=None)`

**Purpose:** Calculate averages for specified symbols over a date range

**Parameters:**
- `symbols`: List of stock symbols (e.g., `['RELIANCE', 'TCS', 'INFY']`)
- `start_date`: Start date in `YYYY-MM-DD` format (optional)
- `end_date`: End date in `YYYY-MM-DD` format (optional)

**Returns:** Dictionary mapping symbols to their averages:
```python
{
  'RELIANCE': {
    'avg_impact_cost': 0.05,
    'avg_free_float_mcap': 1234567890.50,
    'avg_total_market_cap': 1500000000.00,
    'avg_total_traded_value': 5000000.00,
    'days_count': 10  # number of days with data
  },
  'TCS': { ... }
}
```

**How it works with new schema:**
```javascript
// Step 1: Match symbols
{ $match: { symbol: { $in: ['RELIANCE', 'TCS'] } } }

// Step 2: Unwind daily_data array to process each date entry
{ $unwind: '$daily_data' }

// Step 3: Filter by date range (if provided)
{ $match: { 'daily_data.date': { $gte: '2026-02-01', $lte: '2026-02-07' } } }

// Step 4: Calculate averages per symbol
{ $group: {
    _id: '$symbol',
    avg_impact_cost: { $avg: '$daily_data.impact_cost' },
    avg_free_float_mcap: { $avg: '$daily_data.free_float_mcap' },
    ...
}}
```

### 4. **Excel Export with Calculated Averages**

**Updated Function:** `format_dashboard_excel(rows, excel_path, start_date=None, end_date=None)`

**Enhancement:**
When `start_date` or `end_date` is provided, the function now:

1. **Extracts** all symbols from the rows
2. **Calls** `calculate_averages_from_db()` to get averages from database
3. **Replaces** the current values with calculated averages for:
   - `impact_cost`
   - `free_float_mcap`
   - `total_market_cap`
   - `total_traded_value`
4. **Generates** Excel with the averaged values

**Excel Columns:**
- Serial No
- Symbol
- Company name
- Index (DB)
- **avg Impact cost** ← Calculated from DB
- **avg total market cap** ← Calculated from DB
- **Avg Free float market cap** ← Calculated from DB
- **Avg daily traded value** ← Calculated from DB
- Day of Listing
- Broader Index
- listed> 6months
- listed> 1 months
- Ratio of avg free float to avg total market cap
- ratio of free float to avg total market cap

## Usage Flow

### Scenario 1: Single Date Dashboard

```bash
POST /api/nse-symbol-dashboard
{
  "date": "02-Feb-2026",
  "symbols": ["RELIANCE", "TCS", "INFY"]
}
```

**What happens:**
1. Fetches current data from NSE
2. Stores values in both `symbol_metrics` and `symbol_metrics_daily` (with date = 02-Feb-2026)
3. If same symbol + date already exists, **replaces** with new values
4. Returns current values (not averages)

### Scenario 2: Date Range Dashboard with Averages

```bash
POST /api/nse-symbol-dashboard
{
  "start_date": "01-Jan-2026",
  "end_date": "01-Feb-2026",
  "symbols": ["RELIANCE", "TCS", "INFY"],
  "save_to_file": true
}
```

**What happens:**
1. System builds dashboard using batches
2. Each fetch stores daily values in database
3. When generating Excel (via `format_dashboard_excel`):
   - Calculates averages from `symbol_metrics_daily` for date range 01-Jan-2026 to 01-Feb-2026
   - **Replaces** current values with calculated averages
   - Excel shows averaged values over the date range

### Scenario 3: Frontend Multi-Date Build

When frontend calls `/api/nse-symbol-dashboard/save-excel`:

```javascript
{
  "rows": [...],  // All collected rows
  "as_on": "2026-02-01",
  "start_date": "2026-01-01",
  "end_date": "2026-02-01"
}
```

**What happens:**
1. Receives all rows from frontend
2. Calls `format_dashboard_excel` with date range
3. Calculates and replaces with averages from DB
4. Generates Excel with averaged values

## Benefits

### 1. **Historical Data Tracking**
- Every fetch stores a daily snapshot
- Build complete history over time
- Track changes in metrics across dates

### 2. **Accurate Averages**
- No manual calculation needed
- Averages calculated from actual stored data
- Filter by any date range

### 3. **Data Consistency**
- Upsert ensures no duplicates
- Same symbol + date always has one record
- Latest fetch replaces old values

### 4. **Performance**
- MongoDB aggregation is optimized
- Indexed queries for fast retrieval
- Bulk operations for efficient storage

## Database Queries

### Get daily values for a symbol
```javascript
db.symbol_metrics_daily.find({
  symbol: "RELIANCE",
  date: { $gte: "2026-01-01", $lte: "2026-01-31" }
}).sort({ date: 1 })
```

### Get average for a symbol over date range
```javascript
db.symbol_metrics_daily.aggregate([
  {
    $match: {
      symbol: "RELIANCE",
      date: { $gte: "2026-01-01", $lte: "2026-01-31" }
    }
  },
  {
    $group: {
      _id: "$symbol",
      avg_impact_cost: { $avg: "$impact_cost" },
      avg_free_float_mcap: { $avg: "$free_float_mcap" },
      days_count: { $sum: 1 }
    }
  }
])
```

### Check if data exists for a date
```javascript
db.symbol_metrics_daily.find({
  date: "2026-02-02"
}).count()
```

## Code Locations

### Backend Files Modified:
- **`Backend/app.py`**
  - Added `symbol_metrics_daily_collection` initialization (lines ~64-82)
  - Updated `upsert_symbol_metrics()` to store daily values (lines ~289-325)
  - Added `calculate_averages_from_db()` function (lines ~820-870)
  - Updated `format_dashboard_excel()` to use DB averages (lines ~905-1040)

### Key Functions:

1. **`upsert_symbol_metrics(row, source='nse_symbol_metrics')`**
   - Stores symbol metrics in both collections
   - Called after every NSE data fetch

2. **`calculate_averages_from_db(symbols, start_date, end_date)`**
   - Calculates averages from stored daily values
   - Returns dictionary of averages per symbol

3. **`format_dashboard_excel(rows, excel_path, start_date, end_date)`**
   - Generates Excel file
   - Replaces values with DB averages if date range provided

## Testing

### Test 1: Verify Daily Storage
1. Build dashboard for a specific date
2. Check MongoDB:
   ```javascript
   db.symbol_metrics_daily.find({ date: "2026-02-02" }).limit(5)
   ```
3. Verify records are created

### Test 2: Verify Duplicate Handling
1. Build dashboard for same date twice
2. Check MongoDB count - should remain same
3. Verify values are updated (not duplicated)

### Test 3: Verify Average Calculation
1. Build dashboard for multiple dates (e.g., 5 days)
2. Generate Excel with date range
3. Verify Excel shows averaged values
4. Manually verify one symbol's average matches

## Troubleshooting

### Issue: No averages in Excel
**Check:**
1. Database connection is working
2. `symbol_metrics_daily` collection has data
3. Date range is correct format (YYYY-MM-DD)
4. Symbols exist in database

### Issue: Duplicate records
**Check:**
1. Index on `(symbol, date)` is created
2. Upsert is using correct keys

### Issue: Wrong averages
**Check:**
1. Date filter in query is correct
2. Data type of stored values (should be float/number)
3. All dates in range have data

## Future Enhancements

1. **API Endpoint for Averages**
   - Add endpoint to query averages directly
   - Return JSON with averages per symbol

2. **Dashboard Analytics**
   - Show trends over time
   - Compare current vs historical averages

3. **Data Cleanup**
   - Archive old daily data
   - Keep only last N days

4. **Bulk Average Updates**
   - Recalculate averages for all symbols
   - Update Excel files with new averages
