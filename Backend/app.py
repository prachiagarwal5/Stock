from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import pandas as pd
import glob
import json
import csv
from datetime import datetime, timedelta
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import re
from werkzeug.utils import secure_filename
import tempfile
import shutil
from pathlib import Path
import requests
from io import BytesIO, StringIO
import zipfile
from dateutil import parser as date_parser
import numpy as np
from pymongo import MongoClient, UpdateOne
from bson.binary import Binary
from bson import ObjectId
import dotenv
import base64
import math
from google_drive_service import GoogleDriveService
from consolidate_marketcap import MarketCapConsolidator
from nse_symbol_metrics import SymbolMetricsFetcher
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
CORS(app)

# Load environment variables
dotenv.load_dotenv()

# MongoDB connection
try:
    mongo_uri = os.getenv('mongo_URI', 'mongodb://localhost:27017/Stocks')
    mongo_client = MongoClient(mongo_uri)
    db = mongo_client['Stocks']
    excel_results_collection = db['excel_results']
    bhavcache_collection = db['bhavcache']
    symbol_daily_collection = db['symbol_daily']  # per-symbol, per-date values (mcap/pr)
    symbol_aggregates_collection = db['symbol_aggregates']  # per-symbol averages
    symbol_metrics_collection = db['symbol_metrics']  # Symbol dashboard metrics
    # speed-critical indexes
    symbol_daily_collection.create_index(
        [('symbol', 1), ('type', 1), ('date', 1)], name='symbol_type_date', unique=True
    )
    symbol_daily_collection.create_index([('type', 1), ('date', 1)], name='type_date')
    symbol_aggregates_collection.create_index(
        [('symbol', 1), ('type', 1), ('date_range.start', 1), ('date_range.end', 1)],
        name='symbol_type_range', unique=False
    )
    bhavcache_collection.create_index([('type', 1), ('date', 1)], name='type_date_cache')
    print("✅ MongoDB connected successfully")
except Exception as e:
    print(f"⚠️ MongoDB connection failed: {e}")
    db = None
    excel_results_collection = None
    bhavcache_collection = None
    symbol_daily_collection = None
    symbol_aggregates_collection = None
    symbol_metrics_collection = None

# Initialize Google Drive Service
google_drive_service = None
try:
    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
    if os.path.exists(credentials_path):
        google_drive_service = GoogleDriveService(credentials_path)
        # Try to authenticate immediately
        if google_drive_service.authenticate():
            print("✅ Google Drive service initialized successfully")
        else:
            print("⚠️ Google Drive authentication will be attempted on first use")
    else:
        print(f"⚠️ Google credentials not found at {credentials_path}")
        print("   Google Drive features will be unavailable until credentials.json is added")
except Exception as e:
    print(f"⚠️ Google Drive initialization warning: {e}")

HEATMAP_INDICES = [
    'NIFTY 50',
    'NIFTY NEXT 50',
    'NIFTY MIDCAP 50',
    'NIFTY MIDCAP 100',
    'NIFTY MIDCAP 150',
    'NIFTY SMALLCAP 50',
    'NIFTY SMALLCAP 100',
    'NIFTY SMALLCAP 250',
    'NIFTY MIDSMALLCAP 400',
    'NIFTY 100',
    'NIFTY 200',
    'NIFTY500 MULTICAP 50:25:25',
    'NIFTY LARGEMIDCAP 250',
    'NIFTY MIDCAP SELECT'
]

HEATMAP_TYPE_MAP = {
    # All of these are broad-market baskets on NSE heatmap
    'NIFTY 50': 'Broad Market Indices',
    'NIFTY NEXT 50': 'Broad Market Indices',
    'NIFTY MIDCAP 50': 'Broad Market Indices',
    'NIFTY MIDCAP 100': 'Broad Market Indices',
    'NIFTY MIDCAP 150': 'Broad Market Indices',
    'NIFTY SMALLCAP 50': 'Broad Market Indices',
    'NIFTY SMALLCAP 100': 'Broad Market Indices',
    'NIFTY SMALLCAP 250': 'Broad Market Indices',
    'NIFTY MIDSMALLCAP 400': 'Broad Market Indices',
    'NIFTY 100': 'Broad Market Indices',
    'NIFTY 200': 'Broad Market Indices',
    'NIFTY500 MULTICAP 50:25:25': 'Broad Market Indices',
    'NIFTY LARGEMIDCAP 250': 'Broad Market Indices',
    'NIFTY MIDCAP SELECT': 'Broad Market Indices'
}

# Cache for generated index files (download endpoint)
INDEX_FILES = {}

# Custom JSON encoder to handle NaN and Inf values
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.floating):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

def convert_nan_to_none(obj):
    """Recursively convert NaN, inf, and other non-serializable values to None"""
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (list, tuple)):
        return [convert_nan_to_none(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_nan_to_none(value) for key, value in obj.items()}
    elif isinstance(obj, pd.Series):
        return obj.where(pd.notna(obj), None).to_list()
    elif isinstance(obj, pd.DataFrame):
        return obj.where(pd.notna(obj), None).values.tolist()
    return obj


def is_summary_symbol(symbol):
    """Detect summary rows such as Total/Listed (with spaces or punctuation)."""
    if symbol is None:
        return False
    text = str(symbol).strip().upper()
    if not text:
        return False
    normalized = re.sub(r'[^A-Z0-9]', '', text)
    summary_tokens = {'TOTAL', 'LISTED', 'TOTALLISTED', 'LISTEDTOTAL'}
    if normalized in summary_tokens:
        return True
    if text.startswith('TOTAL') or text.startswith('LISTED'):
        return True
    return False


# ===== Index utilities =====
def _make_session(user_agent=None):
    sess = requests.Session()
    if user_agent:
        sess.headers.update({'User-Agent': user_agent})
    return sess


def _prime_cookies(session, headers):
    try:
        session.get('https://www.nseindia.com', headers=headers, timeout=10)
    except Exception as exc:  # best-effort warmup
        print(f"⚠️ NSE cookie warmup failed (indices): {exc}")


def fetch_index_constituents(index_name, session, headers):
    url = f"https://www.nseindia.com/api/equity-stock?index={quote_plus(index_name)}"
    resp = session.get(url, headers=headers, timeout=20)
    if resp.status_code != 200:
        raise ValueError(f"Index fetch failed {resp.status_code} for {index_name}")
    try:
        data = resp.json() if resp.content else {}
    except Exception:
        raise ValueError(f"Invalid JSON for index {index_name}")
    rows = data.get('data') or []
    symbols = []
    for row in rows:
        sym = row.get('symbol') or row.get('symbolName') or row.get('securitySymbol')
        if sym:
            symbols.append(str(sym).strip())
    return symbols


def build_symbol_index_map(index_list):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json,text/plain,*/*',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nseindia.com/market-data/live-market-indices'
    }
    sess = _make_session()
    _prime_cookies(sess, headers)
    mapping = {}
    errors = []
    for idx in index_list:
        try:
            syms = fetch_index_constituents(idx, sess, headers)
            for sym in syms:
                if sym not in mapping:  # primary index only
                    mapping[sym] = idx
        except Exception as exc:
            errors.append({'index': idx, 'error': str(exc)})
    return mapping, errors


def primary_index_map_from_db(symbols):
    """Return latest primary_index per symbol from Mongo (no external calls)."""
    if symbol_metrics_collection is None or not symbols:
        return {}
    mapping = {}
    try:
        cursor = symbol_metrics_collection.find(
            {'symbol': {'$in': symbols}},
            {
                'symbol': 1,
                'primary_index': 1,
                'index': 1,
                'indexList': 1,
                'updated_at': 1,
                'as_on': 1
            }
        ).sort([
            ('updated_at', -1),
            ('as_on', -1)
        ])

        for doc in cursor:
            sym = doc.get('symbol')
            if not sym or sym in mapping:
                continue
            idx = doc.get('primary_index') or doc.get('index')
            if not idx:
                idx_list = doc.get('indexList')
                if isinstance(idx_list, (list, tuple)) and idx_list:
                    idx = idx_list[0]
            if idx:
                mapping[sym] = idx
    except Exception as exc:
        print(f"⚠️ Failed to build primary index map: {exc}")
    return mapping


def _safe_float(val):
    try:
        if val in (None, '', 'NA', 'NaN'):
            return None
        return float(val)
    except Exception:
        return None


def upsert_symbol_daily(symbol, company_name, date_iso, data_type, value, source='consolidation', extra=None):
    if symbol_daily_collection is None or not date_iso:
        return
    try:
        payload = {
            'symbol': symbol,
            'company_name': company_name,
            'date': date_iso,
            'type': data_type,
            'value': _safe_float(value),
            'source': source,
            'updated_at': datetime.now().isoformat()
        }
        if extra:
            payload.update(extra)
        symbol_daily_collection.update_one(
            {'symbol': symbol, 'date': date_iso, 'type': data_type},
            {'$set': payload},
            upsert=True
        )
    except Exception as exc:
        print(f"⚠️ Failed to upsert symbol_daily for {symbol} {date_iso} {data_type}: {exc}")


def upsert_symbol_aggregate(symbol, company_name, data_type, days_with_data, average_value, date_range=None, source='consolidation'):
    if symbol_aggregates_collection is None:
        return
    try:
        payload = {
            'symbol': symbol,
            'company_name': company_name,
            'type': data_type,
            'days_with_data': int(days_with_data or 0),
            'average': _safe_float(average_value),
            'date_range': date_range,
            'source': source,
            'updated_at': datetime.now().isoformat()
        }
        # Use only symbol + type as the key to avoid duplicates across date ranges
        # Each symbol will have exactly one MCAP record and one PR record
        symbol_aggregates_collection.update_one(
            {'symbol': symbol, 'type': data_type},
            {'$set': payload},
            upsert=True
        )
    except Exception as exc:
        print(f"⚠️ Failed to upsert symbol_aggregate for {symbol} {data_type}: {exc}")


def upsert_symbol_metrics(row, source='nse_symbol_metrics'):
    if symbol_metrics_collection is None:
        return
    try:
        symbol = str(row.get('symbol') or '').strip()
        if not symbol:
            return
        payload = dict(row)
        as_on = payload.get('as_on') or payload.get('date') or datetime.now().strftime('%Y-%m-%d')
        if isinstance(as_on, datetime):
            as_on = as_on.strftime('%Y-%m-%d')
        payload['as_on'] = as_on
        payload['source'] = source
        payload['updated_at'] = datetime.now().isoformat()
        symbol_metrics_collection.update_one(
            {'symbol': symbol, 'as_on': as_on},
            {'$set': payload},
            upsert=True
        )
    except Exception as exc:
        print(f"⚠️ Failed to upsert symbol_metrics for {row.get('symbol')}: {exc}")


def persist_consolidated_results(consolidator, data_type, source='consolidation'):
    """Store per-symbol per-date values and averages into MongoDB."""
    if consolidator is None:
        return
    try:
        date_cols = [d[0] for d in consolidator.dates_list]
        date_range = None
        if date_cols:
            try:
                start_iso = datetime.strptime(date_cols[0], '%d-%m-%Y').strftime('%Y-%m-%d')
                end_iso = datetime.strptime(date_cols[-1], '%d-%m-%Y').strftime('%Y-%m-%d')
                date_range = {'start': start_iso, 'end': end_iso}
            except Exception:
                date_range = None

        for _, row in consolidator.df_consolidated.iterrows():
            symbol = str(row.get('Symbol') or '').strip()
            company_name = str(row.get('Company Name') or '').strip()
            if not symbol:
                continue

            avg_val = row.get(consolidator.avg_col)
            days_val = row.get(consolidator.days_col)
            upsert_symbol_aggregate(symbol, company_name, data_type, days_val, avg_val, date_range, source=source)

            for date_str in date_cols:
                val = row.get(date_str)
                if val in (None, ''):
                    continue
                try:
                    date_iso = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                except Exception:
                    date_iso = None
                upsert_symbol_daily(symbol, company_name, date_iso, data_type, val, source=source)
    except Exception as exc:
        print(f"⚠️ Failed to persist consolidated results for {data_type}: {exc}")


def _parse_mcap_date_from_filename(filename):
    match = re.search(r'mcap(\d{2})(\d{2})(\d{4})', filename, re.IGNORECASE)
    if not match:
        return None
    day, month, year = match.groups()
    try:
        return datetime(int(year), int(month), int(day))
    except Exception:
        return None


def collect_symbols_from_files(file_paths):
    symbols = set()
    for path in file_paths:
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip()
            if 'Symbol' in df.columns:
                for sym in df['Symbol'].dropna():
                    sym_clean = str(sym).strip()
                    if sym_clean:
                        symbols.add(sym_clean)
        except Exception as exc:
            print(f"⚠️ Unable to read symbols from {path}: {exc}")
    return sorted(symbols)


def find_mcap_files_in_range(base_dir, start_dt, end_dt):
    pattern = os.path.join(base_dir, 'mcap*.csv')
    files = []
    for path in glob.glob(pattern):
        dt = _parse_mcap_date_from_filename(os.path.basename(path))
        if dt and start_dt <= dt <= end_dt:
            files.append(path)
    return sorted(files)


# ===== Cache helpers =====
def _normalize_iso_date(dt):
    return dt.strftime('%Y-%m-%d')


def get_cached_csv(date_iso, data_type):
    if bhavcache_collection is None:
        return None
    doc = bhavcache_collection.find_one({'date': date_iso, 'type': data_type})
    if not doc:
        return None
    try:
        csv_bytes = doc.get('file_data')
        if not csv_bytes:
            return None
        df = pd.read_csv(BytesIO(csv_bytes))
        return {
            'df': df,
            'records': doc.get('records', len(df)),
            'columns': doc.get('columns', df.columns.tolist()),
            'stored_at': doc.get('stored_at')
        }
    except Exception as e:
        print(f"⚠️ Failed to read cached {data_type} for {date_iso}: {e}")
        return None


def put_cached_csv(date_iso, data_type, df, source='nse'):
    if bhavcache_collection is None or df is None:
        return None
    try:
        csv_buf = BytesIO()
        df.to_csv(csv_buf, index=False)
        bhavcache_collection.update_one(
            {'date': date_iso, 'type': data_type},
            {
                '$set': {
                    'date': date_iso,
                    'type': data_type,
                    'file_data': csv_buf.getvalue(),
                    'records': len(df),
                    'columns': df.columns.tolist(),
                    'stored_at': datetime.now().isoformat(),
                    'source': source
                }
            },
            upsert=True
        )
        return True
    except Exception as e:
        print(f"⚠️ Failed to cache {data_type} for {date_iso}: {e}")
        return None


def bulk_upsert_symbol_daily_from_df(df, date_iso, data_type, source='nse_download'):
    """Fast upsert of per-symbol values into Mongo, avoids per-row round trips."""
    if symbol_daily_collection is None or df is None or df.empty:
        return
    symbol_col = 'Symbol' if data_type == 'mcap' else 'SECURITY'
    name_col = 'Security Name' if data_type == 'mcap' else 'SECURITY'
    value_col = 'Market Cap(Rs.)' if data_type == 'mcap' else 'NET_TRDVAL'
    ops = []
    for _, row in df.iterrows():
        symbol = str(row.get(symbol_col) or '').strip()
        if not symbol or is_summary_symbol(symbol):
            continue
        company_name = str(row.get(name_col) or symbol).strip()
        raw_value = row.get(value_col)
        value = pd.to_numeric(raw_value, errors='coerce')
        if pd.isna(value):
            continue
        ops.append(UpdateOne(
            {'symbol': symbol, 'type': data_type, 'date': date_iso},
            {'$set': {
                'symbol': symbol,
                'company_name': company_name,
                'type': data_type,
                'date': date_iso,
                'value': float(value),
                'source': source,
                'updated_at': datetime.now().isoformat()
            }},
            upsert=True
        ))
    if not ops:
        return
    # chunk to keep payload moderate
    chunk_size = 1000
    for i in range(0, len(ops), chunk_size):
        try:
            symbol_daily_collection.bulk_write(ops[i:i + chunk_size], ordered=False)
        except Exception as exc:
            print(f"⚠️ bulk upsert for {data_type} {date_iso} failed: {exc}")


def build_consolidated_from_cache(date_iso_list, data_type, allow_missing=False, log_fn=None, allowed_symbols=None, symbol_name_map=None):
    """Build consolidated dataframe directly from cached CSVs in Mongo (no filesystem).

    allowed_symbols: filter rows to this set (used to align PR to MCAP symbols)
    symbol_name_map: optional map symbol -> company name (used to carry MCAP names into PR)
    """
    frames = []
    missing_dates = []
    for date_iso in date_iso_list:
        cached = get_cached_csv(date_iso, data_type)
        if not cached or cached.get('df') is None:
            missing_dates.append(date_iso)
            continue
        df = cached['df']
        df.columns = df.columns.str.strip()
        df['_date_iso'] = date_iso
        frames.append(df)

    if missing_dates:
        msg = f"Missing cached {data_type.upper()} for: {', '.join(missing_dates)}"
        if allow_missing:
            if log_fn:
                log_fn(msg)
        else:
            raise ValueError(msg)

    if not frames:
        raise ValueError(f"No cached {data_type.upper()} data available")

    df_all = pd.concat(frames, ignore_index=True)

    if data_type == 'pr':
        symbol_col = 'SECURITY'
        value_col = 'NET_TRDVAL'
        name_col = 'SECURITY'
        avg_col = 'Average Net Traded Value'
    else:
        symbol_col = 'Symbol'
        value_col = 'Market Cap(Rs.)'
        name_col = 'Security Name'
        avg_col = 'Average Market Cap'

    df_all[symbol_col] = df_all[symbol_col].astype(str).str.strip()
    df_all[name_col] = df_all[name_col].astype(str).str.strip()
    df_all[value_col] = pd.to_numeric(df_all[value_col], errors='coerce')

    # Drop summary rows such as Total/Listed
    df_all = df_all[~df_all[symbol_col].apply(is_summary_symbol)]

    if allowed_symbols is not None:
        allowed_set = set(allowed_symbols)
        df_all = df_all[df_all[symbol_col].isin(allowed_set)]
        if df_all.empty:
            raise ValueError(f"No {data_type.upper()} data available for requested symbols")

    df_all['_date_str'] = pd.to_datetime(df_all['_date_iso']).dt.strftime('%d-%m-%Y')

    pivot = df_all.pivot_table(
        index=symbol_col,
        columns='_date_str',
        values=value_col,
        aggfunc='last'
    )

    pivot.reset_index(inplace=True)
    pivot.rename(columns={symbol_col: 'Symbol'}, inplace=True)

    # If we were given an allowed_symbols set, reindex to include all of them (even if PR has gaps)
    if allowed_symbols is not None:
        allowed_order = list(allowed_symbols)
        pivot = pivot.set_index('Symbol').reindex(allowed_order).reset_index()

    if data_type == 'pr' and symbol_name_map:
        # Prefer MCAP-sourced names when provided; fall back to PR names if missing
        name_map = df_all.groupby('SECURITY')[name_col].agg('last').to_dict()
        pivot['Company Name'] = pivot['Symbol'].map(symbol_name_map).fillna(pivot['Symbol'].map(name_map)).fillna(pivot['Symbol'])
    else:
        name_map = df_all.groupby('Symbol' if data_type != 'pr' else 'SECURITY')[name_col].agg('last')
        pivot['Company Name'] = pivot['Symbol'].map(name_map.to_dict()).fillna(pivot['Symbol'])

    # Compute metrics
    date_cols = sorted(
        [c for c in pivot.columns if re.match(r"\d{2}-\d{2}-\d{4}", str(c))],
        key=lambda d: datetime.strptime(d, '%d-%m-%Y')
    )
    # Drop rows that have no PR/MCAP data in any requested date column (prevents empty rows after reindex)
    if allowed_symbols is not None and date_cols:
        pivot = pivot[~pivot[date_cols].isna().all(axis=1)].reset_index(drop=True)
    pivot['Days With Data'] = pivot[date_cols].count(axis=1)
    pivot[avg_col] = pivot[date_cols].mean(axis=1)

    # Sort by average descending, NaN last
    pivot = pivot.sort_values(by=avg_col, ascending=False, na_position='last').reset_index(drop=True)

    # Order columns
    ordered_cols = ['Symbol', 'Company Name', 'Days With Data', avg_col] + date_cols
    pivot = pivot[ordered_cols]

    # Build dates list for downstream formatting/persistence
    dates_list = [(d, datetime.strptime(d, '%d-%m-%Y')) for d in date_cols]

    return pivot, dates_list, avg_col

# Configuration
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'message': 'Market Cap Consolidation Service is running'
    }), 200


@app.route('/api/consolidation-status', methods=['GET'])
def consolidation_status():
    """Check if consolidated MCAP/PR averages exist in the database."""
    if symbol_aggregates_collection is None:
        return jsonify({
            'ready': False,
            'mcap_count': 0,
            'pr_count': 0,
            'message': 'Database not connected'
        }), 200

    try:
        mcap_count = symbol_aggregates_collection.count_documents({'type': 'mcap'})
        pr_count = symbol_aggregates_collection.count_documents({'type': 'pr'})
        
        # Ready only if we have both MCAP and PR averages (at least 100 symbols each)
        ready = mcap_count >= 100 and pr_count >= 100
        
        message = ''
        if ready:
            message = f'✅ Ready: {mcap_count} MCAP and {pr_count} PR symbol averages calculated'
        elif mcap_count < 100 and pr_count < 100:
            message = f'⚠️ Need to export Excel: Only {mcap_count} MCAP and {pr_count} PR averages found (need 100+ each)'
        elif mcap_count < 100:
            message = f'⚠️ MCAP averages incomplete: {mcap_count} found (need 100+)'
        else:
            message = f'⚠️ PR averages incomplete: {pr_count} found (need 100+)'
        
        return jsonify({
            'ready': ready,
            'mcap_count': mcap_count,
            'pr_count': pr_count,
            'message': message
        }), 200
    except Exception as e:
        return jsonify({
            'ready': False,
            'mcap_count': 0,
            'pr_count': 0,
            'message': f'Error checking status: {e}'
        }), 200

def save_excel_to_database(excel_path, filename, metadata):
    """Save Excel file to MongoDB"""
    if excel_results_collection is None:
        return None
    
    try:
        # Read Excel file as binary
        with open(excel_path, 'rb') as f:
            excel_binary = Binary(f.read())
        
        # Create document for MongoDB
        document = {
            'filename': filename,
            'file_data': excel_binary,
            'file_size': os.path.getsize(excel_path),
            'created_at': datetime.now(),
            'metadata': metadata,
            'file_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        # Insert into database
        result = excel_results_collection.insert_one(document)
        print(f"✅ Excel file saved to MongoDB with ID: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"⚠️ Error saving Excel to database: {e}")
        return None

@app.route('/api/preview', methods=['POST'])
def preview():
    """
    Preview endpoint - returns consolidation summary without downloading
    """
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        
        if not files or len(files) == 0:
            return jsonify({'error': 'No files selected'}), 400
        
        request_folder = tempfile.mkdtemp()
        
        try:
            # Save uploaded files
            file_count = 0
            for file in files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(request_folder, filename)
                    file.save(filepath)
                    file_count += 1
            
            if file_count == 0:
                return jsonify({'error': 'No valid CSV files uploaded'}), 400
            
            # Consolidate data for preview
            consolidator = MarketCapConsolidator(request_folder)
            companies_count, dates_count = consolidator.load_and_consolidate_data()
            
            # Get preview data (first 10 rows)
            preview_df = consolidator.df_consolidated.iloc[:10]
            
            # Convert all NaN/inf to None recursively
            preview_data = convert_nan_to_none(preview_df.values.tolist())
            columns = consolidator.df_consolidated.columns.tolist()
            dates = [d[0] for d in consolidator.dates_list]
            
            response_data = {
                'success': True,
                'summary': {
                    'total_companies': len(consolidator.df_consolidated),
                    'total_dates': dates_count,
                    'uploaded_files': file_count,
                    'dates': dates
                },
                'preview': {
                    'columns': columns,
                    'data': preview_data
                }
            }
            
            # Convert any remaining NaN values before returning
            response_data = convert_nan_to_none(response_data)
            
            return jsonify(response_data), 200
        
        finally:
            if os.path.exists(request_folder):
                shutil.rmtree(request_folder)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-nse', methods=['POST'])
def download_nse_data():
    """
    Download market cap data from NSE website
    Expected payload: {"date": "03-Dec-2025"}
    Caches CSVs to Mongo and returns metadata (no sessions).
    """
    try:
        data = request.get_json()
        nse_date = data.get('date', '')  # Format: "03-Dec-2025"
        
        if not nse_date:
            return jsonify({'error': 'Date is required in format DD-Mon-YYYY'}), 400
        
        # Convert NSE date format to DDMMYYYY for filename
        try:
            date_obj = date_parser.parse(nse_date)
            filename_date = date_obj.strftime('%d%m%Y')
            nse_date_formatted = date_obj.strftime('%d-%b-%Y')
        except:
            return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY (e.g., 03-Dec-2025)'}), 400
        
        # NSE API request
        api_url = "https://www.nseindia.com/api/reports"
        
        # Request body as shown in browser developer tools
        params = {
            'archives': json.dumps([{
                "name": "CM - Bhavcopy (PR.zip)",
                "type": "archives",
                "category": "capital-market",
                "section": "equities"
            }]),
            'date': nse_date_formatted,
            'type': 'equities',
            'mode': 'single'
        }
        
        # Headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        print(f"Downloading NSE data for {nse_date_formatted}...")
        
        # Download the ZIP file
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return jsonify({'error': f'NSE API error: {response.status_code}'}), response.status_code
        
        # Extract ZIP file in memory
        try:
            zip_data = BytesIO(response.content)
            with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                # List all files in ZIP
                file_list = zip_ref.namelist()
                print(f"Files in ZIP: {file_list}")
                
                # Look for both mcap and pr files
                mcap_file = None
                pr_file = None
                
                # Find mcap file (market cap)
                for file in file_list:
                    if file.lower().startswith('mcap') and file.lower().endswith('.csv'):
                        mcap_file = file
                        print(f"Found market cap file: {mcap_file}")
                        break
                
                # Find pr file (NET_TRDVAL - net traded value)
                for file in file_list:
                    if file.lower().startswith('pr') and file.lower().endswith('.csv'):
                        pr_file = file
                        print(f"Found PR file: {pr_file}")
                        break
                
                # If mcap not found, look for bhav file as fallback
                if not mcap_file:
                    for file in file_list:
                        if file.lower().endswith('.csv') and ('bhav' in file.lower() or file.lower().startswith('bh')):
                            mcap_file = file
                            print(f"Using Bhavcopy file as mcap: {mcap_file}")
                            break
                
                # Check if we have at least one file
                if not mcap_file and not pr_file:
                    csv_files = [f for f in file_list if f.lower().endswith('.csv')]
                    if csv_files:
                        mcap_file = csv_files[0]
                        print(f"Using fallback CSV file: {mcap_file}")
                    else:
                        return jsonify({'error': f'No CSV files found in ZIP. Files available: {", ".join(file_list)}'}), 400
                
                # Process mcap file
                mcap_df = None
                mcap_filename = f"mcap{filename_date}.csv"
                if mcap_file:
                    try:
                        csv_content = zip_ref.read(mcap_file)
                        mcap_df = pd.read_csv(BytesIO(csv_content))
                        mcap_df.columns = mcap_df.columns.str.strip()
                        print(f"MCAP CSV loaded. Columns: {mcap_df.columns.tolist()}")
                        print(f"MCAP Total records: {len(mcap_df)}")
                        
                        # Validate columns for mcap
                        if 'Symbol' not in mcap_df.columns:
                            return jsonify({'error': f'Symbol column not found in MCAP CSV. Available: {mcap_df.columns.tolist()}'}), 400
                    except Exception as e:
                        return jsonify({'error': f'Error reading MCAP file: {str(e)}'}), 400
                
                # Process pr file
                pr_df = None
                pr_filename = f"pr{filename_date}.csv"
                if pr_file:
                    try:
                        csv_content = zip_ref.read(pr_file)
                        # Some PR files have inconsistent columns; be lenient and skip bad lines.
                        pr_df = pd.read_csv(BytesIO(csv_content), on_bad_lines='skip', engine='python')
                        pr_df.columns = pr_df.columns.str.strip()
                        print(f"PR CSV loaded. Columns: {pr_df.columns.tolist()}")
                        print(f"PR Total records: {len(pr_df)}")
                        
                        # Validate columns for pr (uses SECURITY instead of Symbol)
                        if 'SECURITY' not in pr_df.columns:
                            return jsonify({'error': f'SECURITY column not found in PR CSV. Available: {pr_df.columns.tolist()}'}), 400
                    except Exception as e:
                        print(f"Warning: Error reading PR file: {str(e)}")
                        pr_df = None
                
                # Persist to Mongo cache and symbol_daily for downstream consolidation
                if mcap_df is not None:
                    date_iso = date_obj.strftime('%Y-%m-%d')
                    put_cached_csv(date_iso, 'mcap', mcap_df, source='nse')
                    bulk_upsert_symbol_daily_from_df(mcap_df, date_iso, 'mcap', source='nse_download')
                if pr_df is not None:
                    date_iso = date_obj.strftime('%Y-%m-%d')
                    put_cached_csv(date_iso, 'pr', pr_df, source='nse')
                    bulk_upsert_symbol_daily_from_df(pr_df, date_iso, 'pr', source='nse_download')
                
                return jsonify({
                    'success': True,
                    'message': 'Files downloaded and cached to Mongo',
                    'files': {
                        'mcap': {
                            'filename': mcap_filename,
                            'records': len(mcap_df) if mcap_df is not None else 0,
                            'columns': mcap_df.columns.tolist() if mcap_df is not None else []
                        },
                        'pr': {
                            'filename': pr_filename if pr_df is not None else None,
                            'records': len(pr_df) if pr_df is not None else 0,
                            'columns': pr_df.columns.tolist() if pr_df is not None else []
                        } if pr_df is not None else None
                    },
                    'date': nse_date_formatted
                }), 200
        
        except zipfile.BadZipFile:
            return jsonify({'error': 'Invalid ZIP file received from NSE'}), 400
        except Exception as e:
            return jsonify({'error': f'Error extracting files: {str(e)}'}), 500
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/nse-dates', methods=['GET'])
def get_nse_dates():
    """
    Get available dates for NSE data (last 2 years of trading days)
    Returns list of dates in DD-Mon-YYYY format
    """
    try:
        dates = []
        today = datetime.now()
        
        # Generate dates for last 2 years (approximately 500+ trading days, excluding weekends)
        # 2 years = ~730 days, ~500 trading days (excluding weekends/holidays)
        start_date = today - timedelta(days=730)
        
        current = start_date
        while current <= today:
            # Skip weekends (5=Saturday, 6=Sunday)
            if current.weekday() < 5:
                dates.append(current.strftime('%d-%b-%Y'))
            current += timedelta(days=1)
        
        # Reverse to show most recent first
        dates.reverse()
        
        return jsonify({
            'success': True,
            'dates': dates,
            'today': today.strftime('%d-%b-%Y'),
            'count': len(dates)
        }), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-nse-range', methods=['POST'])
def download_nse_range():
    """
    Download market cap data for a date range from NSE website
    Expected payload: {
        "start_date": "01-Dec-2025",  # Format: DD-Mon-YYYY
        "end_date": "05-Dec-2025",    # Format: DD-Mon-YYYY
        "refresh_mode": "missing_only" or "force"  # Optional, default missing_only
    }
    Returns summary and entries (no sessions; files cached in Mongo).
    """
    try:
        data = request.get_json()
        start_date_str = data.get('start_date', '')
        end_date_str = data.get('end_date', '')
        refresh_mode = data.get('refresh_mode', 'missing_only')  # missing_only | force
        parallel_workers = int(data.get('parallel_workers', 10) or 10)

        if refresh_mode not in ['missing_only', 'force']:
            return jsonify({'error': 'Invalid refresh_mode. Use missing_only or force'}), 400

        if not start_date_str or not end_date_str:
            return jsonify({'error': 'Both start_date and end_date are required'}), 400
        
        # Parse dates
        try:
            start_date = date_parser.parse(start_date_str)
            end_date = date_parser.parse(end_date_str)
        except:
            return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY (e.g., 01-Dec-2025)'}), 400
        
        if start_date > end_date:
            return jsonify({'error': 'start_date cannot be after end_date'}), 400
        
        # Generate list of trading days in range
        current_date = start_date
        trading_dates = []
        
        while current_date <= end_date:
            # Only include weekdays (0-4 = Mon-Fri)
            if current_date.weekday() < 5:
                trading_dates.append(current_date)
            current_date += timedelta(days=1)
        
        if not trading_dates:
            return jsonify({'error': 'No trading days found in the selected range'}), 400
        
        downloads_summary = {
            'total_requested': len(trading_dates),
            'cached_count': 0,
            'fetched_count': 0,
            'failed_count': 0,
            'entries': [],
            'errors': []
        }

        print(f"Downloading NSE data for {len(trading_dates)} trading days (mode={refresh_mode}, workers={parallel_workers})...")

        def process_trade_date(index, trade_date):
            nse_date_formatted = trade_date.strftime('%d-%b-%Y')
            filename_date = trade_date.strftime('%d%m%Y')
            date_iso = _normalize_iso_date(trade_date)

            result = {
                'entries': [],
                'errors': [],
                'cached_count': 0,
                'fetched_count': 0,
                'failed_count': 0
            }

            cached_mcap = None if refresh_mode == 'force' else get_cached_csv(date_iso, 'mcap')
            cached_pr = None if refresh_mode == 'force' else get_cached_csv(date_iso, 'pr')

            need_mcap = refresh_mode == 'force' or cached_mcap is None
            need_pr = refresh_mode == 'force' or cached_pr is None

            mcap_df = cached_mcap['df'] if cached_mcap else None
            pr_df = cached_pr['df'] if cached_pr else None

            if need_mcap or need_pr:
                try:
                    api_url = "https://www.nseindia.com/api/reports"
                    params = {
                        'archives': json.dumps([{
                            "name": "CM - Bhavcopy (PR.zip)",
                            "type": "archives",
                            "category": "capital-market",
                            "section": "equities"
                        }]),
                        'date': nse_date_formatted,
                        'type': 'equities',
                        'mode': 'single'
                    }

                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                    }

                    response = requests.get(api_url, params=params, headers=headers, timeout=30)
                    if response.status_code != 200:
                        result['errors'].append({
                            'date': nse_date_formatted,
                            'error': f'NSE API error: {response.status_code}'
                        })
                        result['failed_count'] += 1
                        return index, result

                    zip_data = BytesIO(response.content)
                    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                        file_list = zip_ref.namelist()

                        mcap_file = None
                        pr_file = None
                        for file in file_list:
                            if file.lower().startswith('mcap') and file.lower().endswith('.csv'):
                                mcap_file = file
                            if file.lower().startswith('pr') and file.lower().endswith('.csv'):
                                pr_file = file

                        if not mcap_file:
                            for file in file_list:
                                if file.lower().endswith('.csv') and ('bhav' in file.lower() or file.lower().startswith('bh')):
                                    mcap_file = file
                                    break

                        if need_mcap and mcap_file:
                            try:
                                csv_content = zip_ref.read(mcap_file)
                                mcap_df = pd.read_csv(BytesIO(csv_content))
                                mcap_df.columns = mcap_df.columns.str.strip()
                                put_cached_csv(date_iso, 'mcap', mcap_df, source='nse')
                            except Exception as e:
                                result['errors'].append({
                                    'date': nse_date_formatted,
                                    'type': 'mcap',
                                    'error': f'Error reading MCAP: {e}'
                                })

                        if need_pr and pr_file:
                            try:
                                csv_content = zip_ref.read(pr_file)
                                # Some PR files have inconsistent columns; be lenient and skip bad lines.
                                pr_df = pd.read_csv(BytesIO(csv_content), on_bad_lines='skip', engine='python')
                                pr_df.columns = pr_df.columns.str.strip()
                                put_cached_csv(date_iso, 'pr', pr_df, source='nse')
                            except Exception as e:
                                result['errors'].append({
                                    'date': nse_date_formatted,
                                    'type': 'pr',
                                    'error': f'Error reading PR: {e}'
                                })
                except Exception as e:
                    result['errors'].append({
                        'date': nse_date_formatted,
                        'error': f'Error fetching ZIP: {str(e)}'
                    })
                    result['failed_count'] += 1
                    return index, result

            if mcap_df is not None:
                bulk_upsert_symbol_daily_from_df(mcap_df, date_iso, 'mcap', source='nse_download')
                status = 'cached' if cached_mcap and refresh_mode != 'force' else 'fetched'
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'mcap',
                    'status': status,
                    'records': len(mcap_df)
                })
                if status == 'cached':
                    result['cached_count'] += 1
                else:
                    result['fetched_count'] += 1
            else:
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'mcap',
                    'status': 'missing'
                })
                result['failed_count'] += 1

            if pr_df is not None:
                bulk_upsert_symbol_daily_from_df(pr_df, date_iso, 'pr', source='nse_download')
                status = 'cached' if cached_pr and refresh_mode != 'force' else 'fetched'
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'pr',
                    'status': status,
                    'records': len(pr_df)
                })
                if status == 'cached':
                    result['cached_count'] += 1
                else:
                    result['fetched_count'] += 1
            else:
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'pr',
                    'status': 'missing'
                })
                result['failed_count'] += 1

            return index, result

        futures = {}
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            for idx, trade_date in enumerate(trading_dates):
                futures[executor.submit(process_trade_date, idx, trade_date)] = idx

            results_by_index = {}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    idx, res = future.result()
                    results_by_index[idx] = res
                except Exception as exc:
                    results_by_index[idx] = {
                        'entries': [],
                        'errors': [{
                            'date': trading_dates[idx].strftime('%d-%b-%Y'),
                            'error': f'Worker error: {exc}'
                        }],
                        'cached_count': 0,
                        'fetched_count': 0,
                        'failed_count': 1
                    }

        for idx in range(len(trading_dates)):
            res = results_by_index.get(idx, None)
            if res is None:
                continue
            downloads_summary['cached_count'] += res['cached_count']
            downloads_summary['fetched_count'] += res['fetched_count']
            downloads_summary['failed_count'] += res['failed_count']
            downloads_summary['entries'].extend(res['entries'])
            downloads_summary['errors'].extend(res['errors'])

        return jsonify({
            'success': True,
            'summary': {
                'total_requested': downloads_summary['total_requested'],
                'cached': downloads_summary['cached_count'],
                'fetched': downloads_summary['fetched_count'],
                'failed': downloads_summary['failed_count'],
                'refresh_mode': refresh_mode,
                'parallel_workers': parallel_workers
            },
            'entries': downloads_summary['entries'],
            'errors': downloads_summary['errors']
        }), 200
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nse-symbol-dashboard', methods=['POST'])
def nse_symbol_dashboard():
    """
    Build a dashboard of impact cost, free float market cap, traded value, and index for symbols.
    Payload options:
    {
        "date": "03-Dec-2025" # use cached MCAP/PR in Mongo
        "start_date": "01-Dec-2025", "end_date": "05-Dec-2025" # optional range scan in Mongo cache
        "symbols": ["ABB", ...],  # optional explicit list
        "save_to_file": true/false  # default true, saves Excel to Mongo and returns download id
    }
    """
    try:
        data = request.get_json() or {}
        date_str = data.get('date')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        save_to_file = data.get('save_to_file', True)
        provided_symbols = data.get('symbols') or []
        top_n = data.get('top_n', 1000)
        top_n_by = data.get('top_n_by') or 'mcap'
        parallel_workers = data.get('parallel_workers', 25)
        chunk_size = data.get('chunk_size', 100)
        as_on = datetime.now().strftime('%Y-%m-%d')
        page = int(data.get('page', 1)) if str(data.get('page', '')).strip() != '' else 1
        page_size = int(data.get('page_size', data.get('max_symbols', 1000))) if str(data.get('page_size', '')).strip() != '' else int(data.get('max_symbols', 1000))
        page_size = max(10, min(page_size, 1000))  # allow up to 1000

        symbols = provided_symbols[:] if provided_symbols else []
        target_files = []
        tag = None

        # Build date_range filter for querying aggregates
        date_range_filter = None
        
        if date_str:
            try:
                date_obj = date_parser.parse(date_str)
            except Exception:
                return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY'}), 400
            tag = date_obj.strftime('%d%m%Y')

        elif start_date_str and end_date_str:
            try:
                start_dt = date_parser.parse(start_date_str)
                end_dt = date_parser.parse(end_date_str)
            except Exception:
                return jsonify({'error': 'Invalid start/end date format. Use DD-Mon-YYYY'}), 400
            if start_dt > end_dt:
                return jsonify({'error': 'start_date cannot be after end_date'}), 400
            tag = f"{start_dt.strftime('%d%m%Y')}_{end_dt.strftime('%d%m%Y')}"
            # Build date range filter to match overlapping ranges in aggregates
            # Match documents where aggregates.date_range overlaps with requested range
            requested_start = start_dt.strftime('%Y-%m-%d')
            requested_end = end_dt.strftime('%Y-%m-%d')
            date_range_filter = {
                'date_range.start': {'$lte': requested_end},    # aggregate starts before or on requested end
                'date_range.end': {'$gte': requested_start}      # aggregate ends after or on requested start
            }
            print(f"[symbol-dashboard] Using date range filter: {date_range_filter}")

        # If symbols not provided, fetch from Mongo aggregates (latest) to avoid filesystem reads
        if not symbols and symbol_aggregates_collection is not None:
            query = {'type': 'mcap'}
            if date_range_filter:
                query.update(date_range_filter)
            agg_symbols = list(symbol_aggregates_collection.distinct('symbol', query))
            symbols = agg_symbols

        # Optionally limit to top N by averages from Mongo aggregates
        if symbol_aggregates_collection is not None:
            try:
                top_n_val = int(top_n) if str(top_n).strip() != '' else 1000
            except Exception:
                top_n_val = 1000
            if top_n_val and top_n_val > 0:
                try:
                    query = {'type': top_n_by}
                    if date_range_filter:
                        query.update(date_range_filter)
                    agg_docs = list(symbol_aggregates_collection.find(query).sort('average', -1).limit(top_n_val))
                    agg_symbols = [doc.get('symbol') for doc in agg_docs if doc.get('symbol')]
                    if agg_symbols:
                        symbols = agg_symbols
                    print(f"[symbol-dashboard] Top {top_n_val} query returned {len(agg_symbols)} symbols")
                except Exception as exc:
                    print(f"⚠️ Unable to fetch top {top_n_val} symbols by {top_n_by}: {exc}")

        if not symbols:
            return jsonify({'error': 'No symbols found. Provide symbols or download MCAP first.'}), 400

        # Drop duplicates and keep order
        symbols = list(dict.fromkeys(symbols))

        total_symbols = len(symbols)
        if total_symbols == 0:
            return jsonify({'error': 'No symbols found. Provide symbols or download MCAP first.'}), 400

        # If top_n provided, override pagination to fetch that many symbols in one batch
        if top_n:
            try:
                top_n_val = int(top_n)
            except Exception:
                top_n_val = None
            if top_n_val and top_n_val > 0:
                symbols = symbols[:top_n_val]
                total_symbols = len(symbols)
                page = 1
                page_size = max(page_size, top_n_val)
        
        total_pages = max(1, math.ceil(total_symbols / page_size))
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_symbols)
        symbols_slice = symbols[start_idx:end_idx]

        fetcher = SymbolMetricsFetcher()

        # Fetch primary_index from DB for the slice (no external calls for indices)
        index_map = primary_index_map_from_db(symbols_slice)

        excel_path = None
        download_name = None
        temp_file = None
        db_id = None
        if save_to_file:
            download_name = f"Symbol_Dashboard_{tag or 'latest'}.xlsx"
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            excel_path = temp_file.name
            temp_file.close()

        print(f"[symbol-dashboard] symbols_total={total_symbols} slice={len(symbols_slice)} top_n={top_n} by={top_n_by} workers={parallel_workers} chunk={chunk_size}")

        # Fetch PR data for % of traded days calculation
        symbol_pr_data = {}
        if symbol_aggregates_collection is not None:
            try:
                pr_query = {
                    'symbol': {'$in': symbols_slice},
                    'type': 'pr'
                }
                if date_range_filter:
                    pr_query.update(date_range_filter)
                pr_docs = list(symbol_aggregates_collection.find(pr_query))
                for doc in pr_docs:
                    sym = doc.get('symbol')
                    if sym:
                        # Get total trading days from date_range if available
                        date_range = doc.get('date_range', {})
                        days_with_data = doc.get('days_with_data', 0)
                        # Calculate total trading days from range
                        total_trading_days = days_with_data  # Default to days with data
                        if date_range and date_range.get('start') and date_range.get('end'):
                            try:
                                start_dt = date_parser.parse(date_range['start'])
                                end_dt = date_parser.parse(date_range['end'])
                                # Count weekdays between dates
                                total_trading_days = sum(1 for d in pd.date_range(start_dt, end_dt) if d.weekday() < 5)
                            except Exception:
                                pass
                        symbol_pr_data[sym] = {
                            'days_with_data': days_with_data,
                            'total_trading_days': total_trading_days,
                            'avg_pr': doc.get('average')
                        }
                print(f"[symbol-dashboard] Loaded PR data for {len(symbol_pr_data)} symbols")
            except Exception as exc:
                print(f"⚠️ Failed to fetch PR data: {exc}")

        # Fetch MCAP data for ratio calculation
        symbol_mcap_data = {}
        if symbol_aggregates_collection is not None:
            try:
                mcap_query = {
                    'symbol': {'$in': symbols_slice},
                    'type': 'mcap'
                }
                if date_range_filter:
                    mcap_query.update(date_range_filter)
                mcap_docs = list(symbol_aggregates_collection.find(mcap_query))
                for doc in mcap_docs:
                    sym = doc.get('symbol')
                    if sym:
                        symbol_mcap_data[sym] = {
                            'avg_mcap': doc.get('average'),
                            'avg_free_float': None  # Will be calculated from metrics if available
                        }
                print(f"[symbol-dashboard] Loaded MCAP data for {len(symbol_mcap_data)} symbols")
            except Exception as exc:
                print(f"⚠️ Failed to fetch MCAP data: {exc}")

        # Run in parallel batches to speed up large symbol lists
        try:
            workers = int(parallel_workers) if str(parallel_workers).strip() != '' else 25
        except Exception:
            workers = 25
        try:
            chunk_val = int(chunk_size) if str(chunk_size).strip() != '' else 100
        except Exception:
            chunk_val = 100

        result = fetcher.build_dashboard(
            symbols_slice,
            excel_path=excel_path,
            max_symbols=None,
            as_of=as_on,
            parallel=True,
            max_workers=max(1, min(workers, 32)),
            chunk_size=max(1, chunk_val),
            symbol_pr_data=symbol_pr_data,
            symbol_mcap_data=symbol_mcap_data
        )

        print(f"[symbol-dashboard] completed fetch rows={len(result.get('rows', []))} errors={len(result.get('errors', []))}")

        # Persist symbol metrics
        for row in result.get('rows', []):
            row = dict(row)
            row['as_on'] = row.get('as_on') or as_on
            sym = row.get('symbol')
            # Prefer DB-sourced primary_index if available
            if sym in index_map:
                row['primary_index'] = index_map[sym]
            upsert_symbol_metrics(row, source='symbol_dashboard')

        download_url = None
        if excel_path and download_name:
            metadata = {
                'tag': tag,
                'symbols_used': len(symbols_slice),
                'total_symbols': total_symbols,
                'page': page,
                'page_size': page_size,
                'as_on': as_on
            }
            db_id = save_excel_to_database(excel_path, download_name, metadata)
            try:
                os.remove(excel_path)
            except Exception:
                pass
            if db_id:
                download_url = f"/api/nse-symbol-dashboard/download?id={db_id}"

        return jsonify({
            'success': True,
            'count': result.get('count', 0),
            'averages': result.get('averages', {}),
            'errors': result.get('errors', []),
            'file': download_name,
            'file_id': str(db_id) if db_id else None,
            'download_url': download_url,
            'symbols_used': len(symbols_slice),
            'total_symbols': total_symbols,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
            'range': {'start': start_idx + 1, 'end': end_idx}
        }), 200
    except Exception as e:
        print(f"Error building symbol dashboard: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nse-symbol-dashboard/download', methods=['GET'])
def download_symbol_dashboard():
    file_id = request.args.get('id')

    if not file_id:
        return jsonify({'error': 'id parameter is required'}), 400

    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500

    try:
        doc = excel_results_collection.find_one({'_id': ObjectId(file_id)})
    except Exception:
        return jsonify({'error': 'Invalid file id'}), 400

    if not doc:
        return jsonify({'error': 'File not found'}), 404

    file_bytes = doc.get('file_data')
    filename = doc.get('filename') or 'Symbol_Dashboard.xlsx'

    if not file_bytes:
        return jsonify({'error': 'File data missing'}), 404

    return send_file(
        BytesIO(file_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


@app.route('/api/dashboard-data', methods=['GET'])
def dashboard_data():
    """Return consolidated dashboard data from Mongo (aggregates + symbol metrics)."""
    if symbol_aggregates_collection is None or symbol_metrics_collection is None:
        return jsonify({'error': 'Database not connected'}), 500

    try:
        limit = int(request.args.get('limit', 100))
        limit = max(1, min(limit, 500))

        def _clean(doc):
            doc = dict(doc)
            doc.pop('file_data', None)
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            if isinstance(doc.get('updated_at'), datetime):
                doc['updated_at'] = doc['updated_at'].isoformat()
            if isinstance(doc.get('as_on'), datetime):
                doc['as_on'] = doc['as_on'].isoformat()
            return doc

        agg_mcap = list(symbol_aggregates_collection.find({'type': 'mcap'}).sort('average', -1).limit(limit))
        agg_pr = list(symbol_aggregates_collection.find({'type': 'pr'}).sort('average', -1).limit(limit))
        metrics = list(symbol_metrics_collection.find({}).sort([('as_on', -1), ('symbol', 1)]).limit(limit))

        return jsonify({
            'success': True,
            'aggregates': {
                'mcap': [_clean(x) for x in agg_mcap],
                'pr': [_clean(x) for x in agg_pr]
            },
            'metrics': [_clean(x) for x in metrics],
            'limit': limit
        }), 200
    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/update-indices', methods=['POST'])
def update_indices():
    if symbol_metrics_collection is None:
        return jsonify({'error': 'Database not connected'}), 500

    try:
        # Build primary index map only from existing Mongo data (no NSE API calls)
        symbol_index_map = {}
        cursor = symbol_metrics_collection.find({}, {
            'symbol': 1,
            'primary_index': 1,
            'index': 1,
            'indexList': 1,
            'updated_at': 1,
            'as_on': 1
        }).sort([
            ('updated_at', -1),
            ('as_on', -1)
        ])

        for doc in cursor:
            sym = doc.get('symbol')
            if not sym or sym in symbol_index_map:
                continue
            idx = doc.get('primary_index') or doc.get('index')
            if not idx:
                idx_list = doc.get('indexList')
                if isinstance(idx_list, (list, tuple)) and idx_list:
                    idx = idx_list[0]
            if idx:
                symbol_index_map[sym] = idx

        if not symbol_index_map:
            return jsonify({'error': 'No index data found in database'}), 404

        bulk_ops = [
            UpdateOne(
                {'symbol': symbol},
                {'$set': {'primary_index': primary_index, 'updated_at': datetime.now().isoformat()}},
                upsert=True
            )
            for symbol, primary_index in symbol_index_map.items()
        ]

        if bulk_ops:
            symbol_metrics_collection.bulk_write(bulk_ops, ordered=False)

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(['symbol', 'primary_index'])
        for sym, idx in sorted(symbol_index_map.items()):
            writer.writerow([sym, idx])
        csv_content = csv_buffer.getvalue()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f'indices_{timestamp}.csv'
        INDEX_FILES['latest'] = {'name': file_name, 'content': csv_content}

        return jsonify({
            'message': 'Indices updated from DB',
            'count': len(symbol_index_map),
            'errors': [],
            'download_path': '/api/download-indices',
            'file_saved': file_name
        }), 200
    except Exception as e:
        print(f"Error in update_indices: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/download-indices', methods=['GET'])
def download_indices():
    cached = INDEX_FILES.get('latest')
    if not cached:
        return jsonify({'error': 'No index file available. Run update first.'}), 404
    buf = BytesIO()
    buf.write(cached['content'].encode('utf-8'))
    buf.seek(0)
    return send_file(
        buf,
        mimetype='text/csv',
        as_attachment=True,
        download_name=cached['name']
    )

@app.route('/api/excel-results', methods=['GET'])
def get_excel_results():
    """Get list of all Excel files stored in database"""
    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        results = list(excel_results_collection.find({}, {
            'file_data': 0,  # Exclude binary data from list
            'file_type': 1,
            'filename': 1,
            'file_size': 1,
            'created_at': 1,
            '_id': 1,
            'metadata': 1
        }).sort('created_at', -1))
        
        # Convert ObjectId to string and datetime to ISO format
        for result in results:
            result['_id'] = str(result['_id'])
            if 'created_at' in result:
                result['created_at'] = result['created_at'].isoformat()
        
        return jsonify({
            'success': True,
            'count': len(results),
            'results': results
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel-results/<file_id>', methods=['GET'])
def download_excel_result(file_id):
    """Download Excel file from database"""
    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        from bson.objectid import ObjectId
        
        # Find file in database
        doc = excel_results_collection.find_one({'_id': ObjectId(file_id)})
        
        if not doc:
            return jsonify({'error': 'File not found'}), 404
        
        # Return binary file data
        return send_file(
            BytesIO(doc['file_data']),
            mimetype=doc.get('file_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
            as_attachment=True,
            download_name=doc.get('filename', 'download.xlsx')
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel-results/<file_id>', methods=['DELETE'])
def delete_excel_result(file_id):
    """Delete Excel file from database"""
    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        from bson.objectid import ObjectId
        
        # Delete file from database
        result = excel_results_collection.delete_one({'_id': ObjectId(file_id)})
        
        if result.deleted_count == 0:
            return jsonify({'error': 'File not found'}), 404
        
        return jsonify({
            'success': True,
            'message': 'File deleted successfully'
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel-results/info/<file_id>', methods=['GET'])
def get_excel_info(file_id):
    """Get metadata about stored Excel file"""
    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        from bson.objectid import ObjectId
        
        # Find file in database
        doc = excel_results_collection.find_one({'_id': ObjectId(file_id)}, {
            'file_data': 0
        })
        
        if not doc:
            return jsonify({'error': 'File not found'}), 404
        
        doc['_id'] = str(doc['_id'])
        if 'created_at' in doc:
            doc['created_at'] = doc['created_at'].isoformat()
        
        return jsonify({
            'success': True,
            'file': doc
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== GOOGLE DRIVE ENDPOINTS ====================

@app.route('/api/google-drive-auth', methods=['POST'])
def google_drive_auth():
    """
    Authenticate with Google Drive
    Returns authentication status and available actions
    """
    try:
        global google_drive_service
        
        if google_drive_service is None:
            credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
            if not os.path.exists(credentials_path):
                return jsonify({
                    'authenticated': False,
                    'error': 'Google credentials not found',
                    'message': 'Please upload credentials.json to enable Google Drive integration'
                }), 400
            
            google_drive_service = GoogleDriveService(credentials_path)
        
        if google_drive_service.authenticate():
            return jsonify({
                'authenticated': True,
                'message': 'Successfully authenticated with Google Drive',
                'automation_folder_id': google_drive_service.get_or_create_automation_folder()
            }), 200
        else:
            return jsonify({
                'authenticated': False,
                'error': 'Authentication failed'
            }), 400
    
    except Exception as e:
        return jsonify({
            'authenticated': False,
            'error': str(e)
        }), 500

@app.route('/api/consolidate', methods=['POST'])
def consolidate():
    """
    Enhanced consolidate endpoint with download destination option
    
    Expected form data:
    - files: Multiple CSV files
    - download_destination: 'local' or 'google_drive'
    - corporate_actions: JSON string with corporate actions config (optional)
    """
    global google_drive_service
    
    try:
        # Check if files were uploaded
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        download_destination = request.form.get('download_destination', 'local')
        
        if not files or len(files) == 0:
            return jsonify({'error': 'No files selected'}), 400
        
        # Validate download destination
        if download_destination not in ['local', 'google_drive']:
            return jsonify({'error': 'Invalid download destination. Use "local" or "google_drive"'}), 400
        
        # Create temporary directory for this request
        request_folder = tempfile.mkdtemp()
        
        try:
            # Save uploaded files
            file_count = 0
            for file in files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(request_folder, filename)
                    file.save(filepath)
                    file_count += 1
            
            if file_count == 0:
                return jsonify({'error': 'No valid CSV files uploaded'}), 400
            
            # Consolidate data
            consolidator = MarketCapConsolidator(request_folder)
            companies_count, dates_count = consolidator.load_and_consolidate_data()
            
            # Create output file
            output_path = os.path.join(request_folder, 'Finished_Product.xlsx')
            consolidator.format_excel_output(output_path)

            # Persist per-symbol values and averages (uploaded files consolidation)
            persist_consolidated_results(consolidator, 'mcap', source='upload_consolidation')
            
            # Prepare metadata
            metadata = {
                'companies_count': companies_count,
                'dates_count': dates_count,
                'dates': [d[0] for d in consolidator.dates_list],
                'files_consolidated': file_count,
                'consolidated_at': datetime.now().isoformat()
            }
            
            # Handle different download destinations
            if download_destination == 'google_drive':
                # Upload to Google Drive
                if google_drive_service is None or not google_drive_service.is_authenticated():
                    # Try to authenticate
                    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
                    if not os.path.exists(credentials_path):
                        return jsonify({
                            'error': 'Google Drive not configured',
                            'message': 'Please add credentials.json to enable Google Drive uploads'
                        }), 400
                    
                    google_drive_service = GoogleDriveService(credentials_path)
                    if not google_drive_service.authenticate():
                        return jsonify({
                            'error': 'Google Drive authentication failed'
                        }), 400
                
                # Upload file to Google Drive
                upload_result = google_drive_service.upload_file(
                    output_path,
                    'Finished_Product.xlsx'
                )
                
                if upload_result is None:
                    return jsonify({
                        'error': 'Failed to upload to Google Drive'
                    }), 500
                
                # Save metadata to database if available
                if excel_results_collection is not None:
                    db_id = save_excel_to_database(output_path, 'Finished_Product.xlsx', metadata)
                
                return jsonify({
                    'success': True,
                    'destination': 'google_drive',
                    'message': 'File uploaded to Google Drive successfully',
                    'file_name': upload_result['file_name'],
                    'file_id': upload_result['file_id'],
                    'web_link': upload_result['web_link'],
                    'metadata': metadata
                }), 200
            
            else:  # local download
                # Save Excel to MongoDB before sending
                db_id = save_excel_to_database(output_path, 'Finished_Product.xlsx', metadata)
                
                # Send file to client
                return send_file(
                    output_path,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name='Finished_Product.xlsx'
                )
        
        finally:
            # Cleanup
            if os.path.exists(request_folder):
                shutil.rmtree(request_folder)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/consolidate-saved', methods=['POST'])
def consolidate_saved():
    """
    Consolidate cached NSE CSVs from Mongo into Excel.

    Payload (JSON):
    {
        "date": "03-Dec-2025"  # optional, single date
        "start_date": "01-Dec-2025", "end_date": "05-Dec-2025"  # optional range
        "file_type": "both" | "mcap" | "pr"  # default both
        "fast_mode": true/false  # default true, skip DB writes when true
    }

    Response: Excel file (zip when both MCAP and PR are produced).
    """
    try:
        try:
            payload = request.get_json() or {}
            date_str = payload.get('date')
            start_date_str = payload.get('start_date')
            end_date_str = payload.get('end_date')
            file_type = payload.get('file_type', 'both')
            # Default to persistence on (fast_mode=False) so aggregates for MCAP/PR get stored
            fast_mode = payload.get('fast_mode', False)
            allow_missing = payload.get('allow_missing', True)

            if file_type not in ['mcap', 'pr', 'both']:
                return jsonify({'error': 'Invalid file_type (mcap, pr, or both)'}), 400

            # Build date list
            date_iso_list = []
            try:
                if date_str:
                    date_dt = date_parser.parse(date_str)
                    date_iso_list = [date_dt.strftime('%Y-%m-%d')]
                elif start_date_str and end_date_str:
                    start_dt = date_parser.parse(start_date_str)
                    end_dt = date_parser.parse(end_date_str)
                    if start_dt > end_dt:
                        return jsonify({'error': 'start_date cannot be after end_date'}), 400
                    current = start_dt
                    while current <= end_dt:
                        if current.weekday() < 5:
                            date_iso_list.append(current.strftime('%Y-%m-%d'))
                        current += timedelta(days=1)
                else:
                    return jsonify({'error': 'Provide either date or start_date/end_date'}), 400
            except Exception:
                return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY (e.g., 03-Dec-2025)'}), 400

            logs = []
            work_dir = tempfile.mkdtemp()
            results = {}

            def add_log(message):
                logs.append(message)

            def make_consolidator_from_cache(data_type, allowed_symbols=None, symbol_name_map=None):
                df, dates_list, avg_col = build_consolidated_from_cache(
                    date_iso_list, data_type, allow_missing=allow_missing, log_fn=add_log,
                    allowed_symbols=allowed_symbols, symbol_name_map=symbol_name_map
                )
                if df is None or df.empty:
                    raise ValueError(f"No {data_type.upper()} data available for requested dates")
                add_log(f"{data_type.upper()} consolidated: {len(df)} companies across {len(dates_list)} dates")
                cons = MarketCapConsolidator(work_dir, file_type=data_type)
                cons.df_consolidated = df
                cons.dates_list = dates_list
                cons.avg_col = avg_col
                cons.days_col = 'Days With Data'
                return cons, len(df), len(dates_list)

            mcap_output_path = None
            mcap_avg_path = None
            pr_output_path = None
            pr_avg_path = None
            mcap_symbols = None

            mcap_name_map = None

            if file_type in ['mcap', 'both']:
                try:
                    consolidator_mcap, companies_count, dates_count = make_consolidator_from_cache('mcap')
                    mcap_output_path = os.path.join(work_dir, 'Market_Cap.xlsx')
                    add_log(f"Creating Excel file: {mcap_output_path}")
                    consolidator_mcap.format_excel_output(mcap_output_path)
                    add_log(f"✓ Excel file created: {mcap_output_path}")
                    mcap_avg_path = mcap_output_path.replace('.xlsx', '_Averages.xlsx')
                    mcap_symbols = set(consolidator_mcap.df_consolidated['Symbol'])
                    mcap_name_map = dict(zip(
                        consolidator_mcap.df_consolidated['Symbol'],
                        consolidator_mcap.df_consolidated['Company Name']
                    ))
                    if not fast_mode:
                        persist_consolidated_results(consolidator_mcap, 'mcap', source='cached_db')
                    results['mcap'] = {
                        'companies': companies_count,
                        'dates': dates_count,
                        'files': len(date_iso_list)
                    }
                except ValueError as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': str(exc)}), 400
                except Exception as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': f'Failed to consolidate MCAP: {exc}'}), 500

            if file_type in ['pr', 'both']:
                try:
                    consolidator_pr, companies_count_pr, dates_count_pr = make_consolidator_from_cache(
                        'pr', allowed_symbols=None, symbol_name_map=mcap_name_map
                    )
                    pr_output_path = os.path.join(work_dir, 'Net_Traded_Value.xlsx')
                    add_log(f"Creating PR Excel file: {pr_output_path}")
                    consolidator_pr.format_excel_output(pr_output_path)
                    add_log(f"✓ PR Excel file created: {pr_output_path}")
                    pr_avg_path = pr_output_path.replace('.xlsx', '_Averages.xlsx')
                    if not fast_mode:
                        persist_consolidated_results(consolidator_pr, 'pr', source='cached_db')
                    results['pr'] = {
                        'companies': companies_count_pr,
                        'dates': dates_count_pr,
                        'files': len(date_iso_list)
                    }
                except ValueError as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': str(exc)}), 400
                except Exception as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': f'Failed to consolidate PR: {exc}'}), 500

            if not mcap_output_path and not pr_output_path:
                shutil.rmtree(work_dir, ignore_errors=True)
                return jsonify({'error': 'No output generated'}), 500

            response = None
            if mcap_output_path and pr_output_path:
                zip_path = os.path.join(work_dir, 'Market_Data.zip')
                add_log(f"Packaging files into zip: {zip_path}")
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(mcap_output_path, arcname='Market_Cap.xlsx')
                    if mcap_avg_path and os.path.exists(mcap_avg_path):
                        zipf.write(mcap_avg_path, arcname='Market_Cap_Averages.xlsx')
                    zipf.write(pr_output_path, arcname='Net_Traded_Value.xlsx')
                    if pr_avg_path and os.path.exists(pr_avg_path):
                        zipf.write(pr_avg_path, arcname='Net_Traded_Value_Averages.xlsx')
                response = send_file(
                    zip_path,
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name='Market_Data.zip'
                )
            elif mcap_output_path:
                zip_path = os.path.join(work_dir, 'Market_Cap.zip')
                add_log(f"Packaging MCAP files into zip: {zip_path}")
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(mcap_output_path, arcname='Market_Cap.xlsx')
                    if mcap_avg_path and os.path.exists(mcap_avg_path):
                        zipf.write(mcap_avg_path, arcname='Market_Cap_Averages.xlsx')
                response = send_file(
                    zip_path,
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name='Market_Cap.zip'
                )
            elif pr_output_path:
                zip_path = os.path.join(work_dir, 'Net_Traded_Value.zip')
                add_log(f"Packaging PR files into zip: {zip_path}")
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(pr_output_path, arcname='Net_Traded_Value.xlsx')
                    if pr_avg_path and os.path.exists(pr_avg_path):
                        zipf.write(pr_avg_path, arcname='Net_Traded_Value_Averages.xlsx')
                response = send_file(
                    zip_path,
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name='Net_Traded_Value.zip'
                )
            if logs:
                safe_log = ' | '.join(logs)
                safe_log = safe_log.encode('ascii', errors='ignore').decode('ascii')
                response.headers['X-Export-Log'] = safe_log

            response.call_on_close(lambda: shutil.rmtree(work_dir, ignore_errors=True))
            return response

        except Exception as e:
            shutil.rmtree(work_dir, ignore_errors=True)
            return jsonify({'error': str(e)}), 500
        response.call_on_close(lambda: shutil.rmtree(work_dir, ignore_errors=True))
        return response

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/google-drive-files', methods=['GET'])
def get_google_drive_files():
    """
    List all files in Google Drive Automation folder
    """
    try:
        if google_drive_service is None or not google_drive_service.is_authenticated():
            return jsonify({
                'error': 'Google Drive not authenticated',
                'files': []
            }), 400
        
        files = google_drive_service.list_files_in_automation_folder()
        
        return jsonify({
            'success': True,
            'count': len(files),
            'files': files
        }), 200
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'files': []
        }), 500

@app.route('/api/google-drive-status', methods=['GET'])
def google_drive_status():
    """
    Check Google Drive integration status
    """
    try:
        is_authenticated = (
            google_drive_service is not None and 
            google_drive_service.is_authenticated()
        )
        
        if is_authenticated:
            automation_folder = google_drive_service.get_or_create_automation_folder()
        else:
            automation_folder = None
        
        return jsonify({
            'authenticated': is_authenticated,
            'service_initialized': google_drive_service is not None,
            'automation_folder_id': automation_folder,
            'credentials_file': 'credentials.json' if os.path.exists('credentials.json') else None
        }), 200
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'authenticated': False
        }), 500


@app.route('/api/heatmap', methods=['GET'])
def heatmap_view():
    """
    Return live index constituents for building an NSE-style heatmap.
    
    Query params:
        - index: Name of index (default: 'NIFTY 50')
    
    Example: /api/heatmap?index=NIFTY%20MIDCAP%2050
    """
    try:
        index_name = request.args.get('index', 'NIFTY 50')
        if index_name not in HEATMAP_INDICES:
            return jsonify({
                'error': 'Unsupported index',
                'available_indices': HEATMAP_INDICES
            }), 400

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/market-data/live-market-indices'
        }

        sess = _make_session()
        _prime_cookies(sess, headers)

        type_name = HEATMAP_TYPE_MAP.get(index_name, 'Broad Market Indices')
        heatmap_url = f"https://www.nseindia.com/api/heatmap-symbols?type={quote_plus(type_name)}&indices={quote_plus(index_name)}"
        
        print(f"[heatmap] Fetching: {heatmap_url}")
        resp = sess.get(heatmap_url, headers=headers, timeout=20)

        # Fallback to equity-stockIndices if heatmap-symbols fails
        used_fallback = False
        data_list = None
        if resp.status_code == 200:
            try:
                data_list = resp.json()
                if isinstance(data_list, list) and len(data_list) > 0:
                    print(f"[heatmap] Got {len(data_list)} symbols from heatmap-symbols API")
            except Exception as e:
                print(f"[heatmap] Failed to parse JSON: {e}")
                data_list = None

        if data_list is None or (isinstance(data_list, list) and len(data_list) == 0):
            used_fallback = True
            fallback_url = f"https://www.nseindia.com/api/equity-stockIndices?index={quote_plus(index_name)}"
            print(f"[heatmap] Fallback to: {fallback_url}")
            resp = sess.get(fallback_url, headers=headers, timeout=20)
            if resp.status_code != 200:
                return jsonify({'error': f'NSE responded with {resp.status_code}'}), resp.status_code
            try:
                data_obj = resp.json() if resp.content else {}
                rows = data_obj.get('data') or []
                print(f"[heatmap] Got {len(rows)} symbols from fallback API")
            except Exception:
                return jsonify({'error': 'Invalid response from NSE'}), 502
            data_list = rows

        constituents = []
        for row in data_list:
            sym = row.get('symbol') or row.get('symbolName') or row.get('identifier') or row.get('securitySymbol')
            if not sym:
                continue
            
            # Handle both heatmap-symbols and equity-stockIndices response formats
            last_price = row.get('lastPrice') or row.get('last')
            p_change = row.get('pChange') or row.get('perChange')
            change_val = row.get('change')
            high_val = row.get('high') or row.get('dayHigh')
            low_val = row.get('low') or row.get('dayLow')
            volume = row.get('totalTradedVolume') or row.get('tradedQuantity')
            traded_value = row.get('quantityTraded') or row.get('totalTradedValue') or row.get('turnoverinlacs')
            vwap = row.get('vwap')
            
            constituents.append({
                'symbol': sym,
                'series': row.get('series'),
                'lastPrice': _safe_float(last_price),
                'pChange': _safe_float(p_change),
                'change': _safe_float(change_val),
                'previousClose': _safe_float(row.get('previousClose')),
                'open': _safe_float(row.get('open')),
                'high': _safe_float(high_val),
                'low': _safe_float(low_val),
                'totalTradedVolume': _safe_float(volume),
                'totalTradedValue': _safe_float(traded_value),
                'vwap': _safe_float(vwap),
                'lastUpdatedTime': row.get('lastUpdatedTime')
            })

        # Derive advances/declines from constituents
        advances = {'advances': 0, 'declines': 0, 'unchanged': 0}
        for item in constituents:
            pc = item.get('pChange')
            if pc is None:
                advances['unchanged'] += 1
                continue
            if pc > 0:
                advances['advances'] += 1
            elif pc < 0:
                advances['declines'] += 1
            else:
                advances['unchanged'] += 1

        payload = {
            'success': True,
            'index': index_name,
            'timestamp': constituents[0].get('lastUpdatedTime') if constituents else None,
            'count': len(constituents),
            'advances': advances,
            'constituents': constituents,
            'available_indices': HEATMAP_INDICES,
            'source': 'heatmap-symbols' if not used_fallback else 'equity-stockIndices'
        }
        return jsonify(convert_nan_to_none(payload)), 200

    except Exception as exc:
        print(f"[heatmap] Error: {exc}")
        return jsonify({'error': str(exc)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
