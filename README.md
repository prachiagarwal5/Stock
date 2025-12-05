# Market Cap Consolidation Tool - README

## 🎯 Overview

This is a **complete full-stack web application** for consolidating daily market cap data with automated NSE (National Stock Exchange) integration. The system includes:

- ✅ **React Frontend** - Beautiful, responsive web UI with 5-tab interface
- ✅ **Flask Backend** - REST API with automatic NSE data scraping
- ✅ **Automated NSE Downloads** - Single date or bulk date range downloads
- ✅ **Professional Excel Export** - Multi-date consolidation with formatting
- ✅ **Corporate Actions Support** - Stock splits, name changes, delistings
- ✅ **Real-time Progress Tracking** - Live status updates for bulk operations
- ✅ **Frozen Panes & Styling** - Professional Excel output

---

## 📦 Project Structure

```
Proj01/
├── Backend/
│   ├── app.py                    # Flask REST API server
│   ├── consolidate_marketcap.py  # Data consolidation logic
│   ├── requirements.txt          # Python dependencies
│   ├── venv/                     # Virtual environment
│   └── temp/                     # Temporary files (auto-cleaned)
├── Frontend/
│   ├── src/
│   │   ├── App.jsx              # React main component (5 tabs)
│   │   ├── App.css              # Responsive styling
│   │   └── main.jsx             # Entry point
│   ├── package.json             # Node dependencies
│   ├── vite.config.js           # Vite configuration
│   └── node_modules/            # Installed packages
├── nosubject/                    # Data files directory
│   └── *.csv                     # Input CSV files
└── README.md                     # This file
```

---

## 🚀 Getting Started (Full Stack Setup)

### Prerequisites:
- Python 3.13+ installed
- Node.js 18+ installed
- macOS/Linux terminal

### Step 1: Setup Backend Virtual Environment

```bash
cd /Users/vinayak/Desktop/Proj01/Backend

# Create virtual environment (if not already created)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies from requirements.txt
pip install -r requirements.txt

# Verify installation
pip list
```

**Expected Output:** Should show Flask, pandas, openpyxl, requests, beautifulsoup4, python-dateutil, and other packages.

### Step 2: Start Backend Server

```bash
# Make sure you're in Backend directory with venv activated
cd /Users/vinayak/Desktop/Proj01/Backend
source venv/bin/activate

# Start Flask server
python app.py
```

**Expected Output:**
```
 * Serving Flask app 'app'
 * Debug mode: on
 * Running on http://127.0.0.1:5000
 * Debugger is active!
```

The backend will be running on **http://localhost:5000**

### Step 3: Setup Frontend & Start React Server

**In a new terminal:**

```bash
cd /Users/vinayak/Desktop/Proj01/Frontend

# Install Node dependencies (if not already installed)
npm install

# Start React development server
npm run dev
```

**Expected Output:**
```
  VITE v4.5.14  ready in XXX ms
  ➜  Local:   http://localhost:3000/
```

The frontend will be running on **http://localhost:3000**

### Step 4: Open the Application

Open your browser and go to: **http://localhost:3000**

---

## 🎯 Application Features (4 Tabs)

### 1. 🔽 Download from NSE (Single Date)
- Download market cap data from NSE for a specific date
- Choose from dropdown or type any date (last 2 years available)
- File saved as: `mcapDDMMYYYY.csv`
- Automatic trading days only (no weekends/holidays)

### 2. 📅 Date Range Download (Bulk)
- Download data for multiple dates at once
- Enter start date and end date
- Real-time progress with success/failure statistics
- Automatic file naming and consolidation

### 3. 📤 Upload & Process
- Upload CSV files manually
- Preview data before consolidation

### 4. 👁️ Preview
- Preview consolidated data before export
- Download Excel file with all formatting
- Frozen panes and professional styling included

---

## 🔄 Complete Workflow Example

### Scenario: Download data for last 5 trading days and generate Excel

**Step 1:** Open http://localhost:3000 in browser

**Step 2:** Go to "📅 Date Range Download" tab

**Step 3:** 
- Enter Start Date: 25-Nov-2025
- Enter End Date: 05-Dec-2025
- Click "Download"

**Step 5:** Go to "👁️ Preview" tab and review data

**Step 6:** Click "Download Excel" to save `Finished_Product.xlsx`

**Step 7:** Open Excel file and view consolidated data with all dates as columns

---

## 🛠 Backend REST API Endpoints

### 1. Health Check
```bash
GET /health
# Response: {"status": "ok"}
```

### 2. Get Available NSE Dates
```bash
GET /api/nse-dates
# Response: {"dates": ["05-Dec-2025", "04-Dec-2025", ..., "05-Dec-2023"], "count": 500+, "today": "05-Dec-2025"}
```

### 3. Download Single Date from NSE
```bash
POST /api/download-nse
# Request: {"date": "05-Dec-2025", "save_to_file": true}
# Response: {"success": true, "filename": "mcap05122025.csv", "records": 2750}
```

### 4. Download Date Range from NSE
```bash
POST /api/download-nse-range
# Request: {"start_date": "01-Dec-2025", "end_date": "05-Dec-2025", "save_to_file": true}
# Response: {
#   "summary": {"successful": 4, "failed": 0, "total_requested": 5},
#   "files": [{"date": "05-Dec-2025", "filename": "mcap05122025.csv", "records": 2750}],
#   "errors": []
# }
```

### 5. Preview Consolidation
```bash
POST /api/preview
# Request: FormData with CSV files
# Response: Preview of consolidated data
```

### 6. Generate Excel
```bash
POST /api/consolidate
# Request: FormData with CSV files + corporate actions (optional)
# Response: Excel file download
```

---

## 📋 Manual Backend Startup (Without venv)

If you prefer using system Python (packages already installed):

```bash
cd /Users/vinayak/Desktop/Proj01/Backend
/opt/miniconda3/bin/python app.py
```

Or if using conda:

```bash
conda run -n base python /Users/vinayak/Desktop/Proj01/Backend/app.py
```

---

## 🎯 Quick Start (One-Command Setup)

---

## 📂 Traditional CLI Usage (Without Web App)

If you prefer running the consolidation from command line instead of the web interface:

```bash
cd /Users/vinayak/Desktop/Proj01
source venv/bin/activate
python3 consolidate_marketcap.py
```

This will consolidate all CSV files in `/nosubject/` and generate `Finished_Product.xlsx`

---

## 🛠 Troubleshooting

### Backend Issues

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: No module named 'flask'` | Make sure to activate venv: `source venv/bin/activate` OR use full Python path: `/opt/miniconda3/bin/python app.py` |
| Backend won't start on port 5000 | Check if port is in use: `lsof -i :5000` and kill if needed: `kill -9 <PID>` |
| Import errors for pandas/openpyxl | Run `pip install -r requirements.txt` again with venv activated |
| Connection refused on http://localhost:5000 | Make sure backend server is running in another terminal |

### Frontend Issues

| Issue | Solution |
|-------|----------|
| `npm: command not found` | Install Node.js from https://nodejs.org/ |
| `port 3000 already in use` | Kill existing process: `lsof -i :3000` then `kill -9 <PID>` |
| `npm ERR! 404 Not Found` | Delete `node_modules` and `package-lock.json`, then run `npm install` again |
| Files not uploading | Check that backend is running on port 5000 and CORS is enabled |

### Data Issues

| Issue | Solution |
|-------|----------|
| CSV file not recognized | Ensure filename matches `mcapDDMMYYYY.csv` pattern |
| NSE download fails | Check internet connection and verify date is a trading day (not weekend/holiday) |
| Column not found error | Verify CSV has columns: `Symbol`, `Security Name`, `Market Cap(Rs.)` |
| Corporate actions not applied | Check JSON format in corporate_actions.json, use `DD-MM-YYYY` date format |

---

## 📊 CSV Input Format (For Manual Uploads)

### Required Columns:
- `Symbol` - Stock ticker symbol (e.g., TCSM, RELIANCE)
- `Security Name` or `Company Name` - Full company name
- `Market Cap(Rs.)` - Market capitalization value in rupees

### File Naming:
- Format: `mcapDDMMYYYY.csv`
- Examples:
  - `mcap05122025.csv` = 05 DEC 2025
  - `mcap31122025.csv` = 31 DEC 2025
  - `mcap01012026.csv` = 01 JAN 2026

### File Location (For CLI usage):
- Must be in: `/Users/vinayak/Desktop/Proj01/nosubject/`

---

## 🛡️ Advanced Configuration

### Backend Configuration (app.py)

Key settings you can modify:

```python
# Flask debug mode
app.run(debug=True)  # Set to False for production

# CORS settings
CORS(app, resources={r"/api/*": {"origins": "*"}})  # Restrict as needed

# File size limits
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max

# Temp file cleanup
temp_dir = "temp/"  # Where temporary files are stored
```

### Frontend Configuration (vite.config.js)

```javascript
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    open: true
  }
})
```

---

## 🔐 Data & Security

### Blank Cell Handling:
- If symbol not in a date's CSV → cell is BLANK (not 0 or N/A)
- Preserves data quality and prevents erroneous calculations

### File Backup:
- Excel files downloaded from UI are saved locally in your Downloads folder
- Data uploaded to backend is processed temporarily and auto-cleaned after generation
- CSV files in `/nosubject/` folder are preserved and never deleted

### NSE Data:
- Downloaded files are automatically validated against NSE column format
- Date filtering ensures only trading days (no weekends/holidays)
- All data remains local - nothing is sent to external servers except NSE API

---

## 📈 Use Cases

1. **Daily Tracking:** Monitor market cap changes across all companies
2. **Trend Analysis:** Compare values across dates for single companies
3. **Data Export:** Extract to analytics tools or databases
4. **Bulk Data Collection:** Download weeks/months of data automatically
5. **Reporting:** Create presentations with historical data

---

## 🎓 Documentation Files

Included documentation files for additional reference:

1. **README.md** - This file (full documentation)
2. **QUICK_START.md** - 5-minute quick reference
3. **USAGE_GUIDE.md** - Detailed feature documentation
4. **NSE_INTEGRATION_GUIDE.md** - NSE data source details
5. **FULLSTACK_SETUP.md** - Complete setup walkthrough
6. **FULLSTACK_QUICK_START.md** - Quick command reference

---

## ✨ Technology Stack

### Backend:
- **Framework:** Flask 3.1.1
- **Language:** Python 3.13
- **Data Processing:** pandas 2.3.1
- **Excel Generation:** openpyxl 3.1.5
- **Web Scraping:** requests 2.32.3, beautifulsoup4 4.13.4
- **CORS Support:** Flask-CORS 6.0.1
- **Date Utilities:** python-dateutil 2.9.0

### Frontend:
- **Framework:** React 18.2.0
- **Build Tool:** Vite 4.5.14
- **Styling:** CSS3 with gradients and animations
- **HTTP Client:** Fetch API

### Database/Storage:
- **Local CSV Files:** `/nosubject/` folder
- **Excel Output:** Generated on-demand
- **Temporary Files:** Auto-cleaned after processing

---

## 🎯 Key Features Comparison

| Feature | CLI Tool | Web App | NSE Integration |
|---------|----------|---------|-----------------|
| Upload CSV files | ✅ (folder) | ✅ (drag-drop) | ✅ (auto-download) |
| Single date processing | ✅ | ✅ | ✅ |
| Bulk date processing | ❌ | ✅ | ✅ |
| Real-time progress | ❌ | ✅ | ✅ |
| Excel export | ✅ | ✅ | ✅ |
| Web interface | ❌ | ✅ | ✅ |
| Data preview | ❌ | ✅ | ✅ |
| REST API | ❌ | ✅ | ✅ |

---

## 🚀 Performance Tips

### For Large Date Ranges:
1. Download in smaller chunks (5-10 trading days at a time)
2. Browser may take time to download if Excel is large (>50MB)
3. Consolidating 50+ dates might take 30-60 seconds

### For System Performance:
1. Keep temp files clean: Files are auto-deleted after download
2. Close browser tabs if system is slow
3. Use Chrome/Firefox for best performance (Safari may be slower)

### For NSE Optimization:
1. NSE servers may be slow during market hours (9:30-15:30 IST)
2. Download data after market close for faster speeds
3. Check your internet connection if downloads fail

---

## 📞 Support & Help

### Common Questions

**Q: Where are my downloaded Excel files?**  
A: Check your browser's Downloads folder (usually `~/Downloads/` on Mac)

**Q: Can I download data from a year ago?**  
A: Yes! The date picker shows last 2 years of trading days. Select from dropdown or type any date in format `DD-Mon-YYYY` (e.g., 25-Nov-2024)

**Q: What happens if NSE is down?**  
A: Backend will return error with details. Try again later or upload CSV files manually

**Q: Can I run this without the web interface?**  
A: Yes! Use the CLI: `cd Backend && source venv/bin/activate && python3 consolidate_marketcap.py`

**Q: Is my data secure?**  
A: All data is processed locally on your machine. No data is sent anywhere except NSE API calls for download

---

## 📝 Version History

- **v2.0** (Current) - Full-stack web app with React + Flask, NSE integration, date range downloads, REST API
- **v1.0** (Initial) - CLI-based Python script with manual CSV processing

---

## 🎉 You're Ready!

Everything is set up and ready to use. Just:

1. **Start Backend:** `cd Backend && source venv/bin/activate && python app.py`
2. **Start Frontend:** `cd Frontend && npm run dev` (in new terminal)
3. **Open Browser:** http://localhost:3000
4. **Download & Consolidate:** Use the web interface or upload CSVs manually

**Questions or issues?** Check the troubleshooting section above.

---

*Created: December 5, 2025*  
*Solution: Market Cap Consolidation Tool v2.0*  
*Full-stack React + Flask application with NSE integration*  
*GitHub Repository: https://github.com/vinayaksingh930/Stock*

