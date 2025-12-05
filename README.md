# Market Cap Consolidation Tool - README

## 🎯 Overview

This is a complete automated solution for consolidating daily market cap data across multiple CSV files into a single, professionally formatted Excel workbook. It intelligently handles:

- ✅ **Automatic date detection** from filename patterns
- ✅ **Multi-date consolidation** with proper date-wise columns
- ✅ **Corporate actions** (stock splits, name changes, delistings)
- ✅ **Auto-refresh** when new CSV files are added
- ✅ **Professional Excel formatting** with frozen panes and styling
- ✅ **Blank cell handling** for missing data

---

## 📦 What You Get

### Files Created:

1. **consolidate_marketcap.py** - Main Python script
2. **requirements.txt** - Python dependencies list
3. **venv/** - Virtual environment with all packages
4. **corporate_actions.json** - Configuration template for corporate actions
5. **Finished_Product.xlsx** - Generated Excel consolidation file
6. **update_data.sh** - One-click update script (Mac/Linux)
7. **USAGE_GUIDE.md** - Comprehensive documentation
8. **QUICK_START.md** - Quick reference guide
9. **README.md** - This file

---

## 🚀 Quick Start

### Every time you get new data:

```bash
# Method 1: Shell script (easiest on Mac)
/Users/vinayak/Desktop/Proj01/update_data.sh

# Method 2: Manual command
cd /Users/vinayak/Desktop/Proj01
source venv/bin/activate
python3 consolidate_marketcap.py
```

Then open: `/Users/vinayak/Desktop/Proj01/nosubject/Finished_Product.xlsx`

---

## 📂 How It Works

### Input:
- Daily CSV files: `mcapDDMMYYYY.csv` (e.g., `mcap18112025.csv`)
- Located in: `/Users/vinayak/Desktop/Proj01/nosubject/`
- Must contain: Symbol, Security Name, Market Cap(Rs.)

### Processing:
1. Scans folder for all CSV files matching `mcap*.csv` pattern
2. Extracts date from filename (DDMMYYYY format)
3. Consolidates market cap data by symbol
4. Applies corporate actions if configured
5. Generates formatted Excel file

### Output:
- **File:** `Finished_Product.xlsx`
- **Location:** `/Users/vinayak/Desktop/Proj01/nosubject/`
- **Format:** Symbol | Company Name | Date1 | Date2 | Date3 | ...

---

## 🔄 Workflow Example

### Day 1: Initial Setup
```bash
cd /Users/vinayak/Desktop/Proj01
source venv/bin/activate
python3 consolidate_marketcap.py
# Creates Finished_Product.xlsx with data from mcap10112025.csv through mcap17112025.csv
```

### Day 2: New data arrives (mcap18112025.csv)
```bash
# Copy mcap18112025.csv to /Users/vinayak/Desktop/Proj01/nosubject/
/Users/vinayak/Desktop/Proj01/update_data.sh
# Automatically updates Finished_Product.xlsx with new date column
```

### Day 3: Corporate action occurs (Tata Motors splits to TMPV + TMCV on 20-11-2025)
```bash
# Edit corporate_actions.json:
{
  "splits": [{
    "old_symbol": "TATAMOTOR",
    "new_symbols": ["TMPV", "TMCV"],
    "split_date": "20-11-2025"
  }]
}

# Run update
/Users/vinayak/Desktop/Proj01/update_data.sh
# TATAMOTOR row now blanks out before 20-11-2025
# TMPV and TMCV rows show data from 20-11-2025 onwards
```

---

## 🛠 Configuration: Corporate Actions

### File: `corporate_actions.json`

Located at: `/Users/vinayak/Desktop/Proj01/nosubject/corporate_actions.json`

### Three Types of Actions:

#### 1. Stock Split/Demerger
```json
"splits": [
  {
    "old_symbol": "PARENT",
    "new_symbols": ["CHILD1", "CHILD2"],
    "split_date": "DD-MM-YYYY"
  }
]
```
**Effect:** Old symbol blanks BEFORE split date; new symbols start FROM split date

#### 2. Name/Symbol Change
```json
"name_changes": [
  {
    "old_symbol": "OLDNAME",
    "new_symbol": "NEWNAME",
    "change_date": "DD-MM-YYYY"
  }
]
```
**Effect:** Old symbol blanks BEFORE change date; new symbol starts FROM change date

#### 3. Delisting
```json
"delistings": [
  {
    "symbol": "DELISTED",
    "delisting_date": "DD-MM-YYYY"
  }
]
```
**Effect:** Symbol blanks FROM delisting date onwards (optional feature)

---

## 📊 Excel Output Features

### Structure:
- **Column A:** Stock Symbol (e.g., RELIANCE, INFY, TCS)
- **Column B:** Company Full Name
- **Columns C+:** Market Cap values for each date (e.g., 10-11-2025, 11-11-2025, etc.)

### Formatting:
- ✓ Blue header row with white text
- ✓ Dates sorted chronologically (left to right)
- ✓ Market cap numbers right-aligned with thousand separators
- ✓ First row frozen (always visible when scrolling down)
- ✓ First two columns frozen (always visible when scrolling right)
- ✓ Blank cells where data unavailable (not zeros)

---

## ⚙️ Technical Details

### Python Requirements:
- Python 3.13+ (already installed in venv)
- pandas 2.3.3 - Data manipulation
- openpyxl 3.1.5 - Excel file generation

### Virtual Environment:
- Location: `/Users/vinayak/Desktop/Proj01/venv/`
- Already set up and ready to use
- All packages pre-installed

### How to Verify:
```bash
cd /Users/vinayak/Desktop/Proj01
source venv/bin/activate
pip list  # Shows installed packages
```

---

## 📋 CSV Input Format

### Required Columns:
- `Symbol` - Stock ticker symbol (e.g., TCSM, RELIANCE)
- `Security Name` - Full company name
- `Market Cap(Rs.)` - Market capitalization value

### File Naming:
- Format: `mcapDDMMYYYY.csv`
- Examples:
  - `mcap10112025.csv` = 10 NOV 2025
  - `mcap31122025.csv` = 31 DEC 2025
  - `mcap01012026.csv` = 01 JAN 2026

### File Location:
- Must be in: `/Users/vinayak/Desktop/Proj01/nosubject/`

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | Run: `source venv/bin/activate` first |
| Script doesn't find CSV files | Check file names match `mcap*.csv` pattern |
| Corporate action not applied | Verify date format is `DD-MM-YYYY` in JSON |
| Excel file not updating | Run script from correct directory |
| Column name issue | Check CSV has columns: Symbol, Security Name, Market Cap(Rs.) |
| New date column missing | Verify CSV file placed in correct folder |

---

## 🔐 Data Integrity

### Blank Cell Handling:
- If symbol not in a date's CSV → cell is BLANK (not 0 or N/A)
- Preserves data quality and prevents erroneous calculations

### Corporate Actions:
- Cells before action date → BLANK (not deleted)
- Cells from action date → Data populated
- Allows proper historical tracking

### File Backup:
- Previous Excel versions are automatically overwritten
- To keep history, manually save copies with dates:
  - `Finished_Product_20251205.xlsx`
  - `Finished_Product_20251206.xlsx`

---

## 📈 Use Cases

1. **Daily Tracking:** Monitor market cap changes across all companies
2. **Trend Analysis:** Compare values across dates for single companies
3. **Corporate Action Auditing:** Verify splits/delistings handled correctly
4. **Data Export:** Extract to analytics tools or databases
5. **Reporting:** Create presentations with historical data

---

## 🎓 Learning Resources

### Files to Read:
1. **QUICK_START.md** - 5-minute getting started
2. **USAGE_GUIDE.md** - Complete feature documentation
3. **consolidate_marketcap.py** - Code comments explain logic

### Common Tasks:

**Adding new date's data:**
```bash
# 1. Copy mcapNEWDATE.csv to /nosubject/
# 2. Run: /Users/vinayak/Desktop/Proj01/update_data.sh
# 3. Open Finished_Product.xlsx - new date column appears automatically!
```

**Handling stock split:**
```bash
# Edit corporate_actions.json to add split entry
# Run: /Users/vinayak/Desktop/Proj01/update_data.sh
# Old symbol blanks before split, new symbols show from split date
```

**Recreating from scratch:**
```bash
# Delete Finished_Product.xlsx
# Run: /Users/vinayak/Desktop/Proj01/update_data.sh
# Regenerated automatically from all CSV files
```

---

## 📞 Support

### When Issues Occur:

1. **Check the logs:** Look at terminal output when running script
2. **Verify inputs:** Ensure CSV files have correct format
3. **Review configuration:** Check corporate_actions.json syntax
4. **Re-run script:** Often solves temporary issues

### Debug Mode:
```bash
cd /Users/vinayak/Desktop/Proj01
source venv/bin/activate
python3 consolidate_marketcap.py  # Shows detailed output
```

---

## ✨ Features Summary

| Feature | Details |
|---------|---------|
| **Auto-detection** | Scans folder for `mcap*.csv` files |
| **Date parsing** | Extracts date from filename (DDMMYYYY) |
| **Consolidation** | Combines all dates into single Excel file |
| **Formatting** | Professional Excel with headers and styling |
| **Corporate actions** | Handles splits, name changes, delistings |
| **Blank handling** | Leaves cells blank where data missing |
| **Frozen panes** | Easy scrolling with visible headers |
| **Auto-refresh** | Re-run script anytime to update with new data |

---

## 🎉 You're All Set!

Everything is configured and ready to use. Just:
1. Add new CSV files to `/nosubject/`
2. Run `/Users/vinayak/Desktop/Proj01/update_data.sh`
3. View the updated `Finished_Product.xlsx`

**Start using it today!**

---

*Created: December 5, 2025*  
*Solution: Market Cap Consolidation Tool v1.0*  
*Python-based automation with Excel integration*


# Terminal 1 - Backend

cd /Users/vinayak/Desktop/Proj01/Backend
source venv/bin/activate
pip install -r requirements.txt
python app.py


# Terminal 2 - Frontend
cd /Users/vinayak/Desktop/Proj01/Frontend
npm install
npm run dev

# Then open http://localhost:3000# Stock
