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
import time
from dateutil import parser as date_parser
import numpy as np
from pymongo import MongoClient, UpdateOne
from bson.binary import Binary
from bson import ObjectId
import dotenv
import base64
import math
from consolidate_marketcap import MarketCapConsolidator
from nse_symbol_metrics import SymbolMetricsFetcher
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed
from memory_optimized_export import MemoryOptimizedExporter, ChunkedDataProcessor, get_memory_usage_mb
import gc

app = Flask(__name__)

# Enhanced CORS configuration for production deployment
CORS(app, 
     resources={r"/*": {
         "origins": "*",
         "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         "allow_headers": ["Content-Type", "Authorization", "Accept", "X-Requested-With"],
         "expose_headers": ["Content-Disposition", "X-Export-Log"],
         "supports_credentials": False,
         "max_age": 3600
     }})

@app.after_request
def add_cors_headers(response):
    # Ensure CORS headers are present (fallback for production)
    if 'Access-Control-Allow-Origin' not in response.headers:
        response.headers['Access-Control-Allow-Origin'] = '*'
    
    if 'Access-Control-Allow-Methods' not in response.headers:
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    
    if 'Access-Control-Allow-Headers' not in response.headers:
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Accept, X-Requested-With'
    
    # Add expose headers
    expose = response.headers.get('Access-Control-Expose-Headers', '')
    needed = ['Content-Disposition', 'X-Export-Log']
    if expose:
        existing = [h.strip() for h in expose.split(',') if h.strip()]
    else:
        existing = []
    for h in needed:
        if h not in existing:
            existing.append(h)
    response.headers['Access-Control-Expose-Headers'] = ', '.join(existing)
    
    return response

@app.before_request
def handle_preflight():
    """Handle OPTIONS preflight requests"""
    if request.method == 'OPTIONS':
        response = app.make_default_options_response()
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Accept, X-Requested-With'
        response.headers['Access-Control-Max-Age'] = '3600'
        return response

@app.route("/")
def home():
    return "Backend is running 🚀"

# Load environment variables
dotenv.load_dotenv()

# MongoDB connection
try:
    # Hardcoded MongoDB connection string (as per user request)
    mongo_uri = "mongodb+srv://prachiagrawal509:BSzCRUTG8F7voUBv@cluster0.kfbej.mongodb.net/Stocks?retryWrites=true&w=majority"
    
    print(f"🔄 Connecting to MongoDB...")
    mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    # Test the connection
    mongo_client.admin.command('ping')
    db = mongo_client['Stocks']
    excel_results_collection = db['excel_results']
    bhavcache_collection = db['bhavcache']
    symbol_daily_collection = db['symbol_daily']  # per-symbol, per-date values (mcap/pr)
    symbol_aggregates_collection = db['symbol_aggregates']  # per-symbol averages
    symbol_metrics_collection = db['symbol_metrics']  # Symbol dashboard metrics
    symbol_metrics_daily_collection = db['symbol_metrics_daily']  # Daily impact_cost and free_float_mcap
    nifty_indices_collection = db['nifty_indices']  # Nifty index constituent mappings
    
    print(f"🔄 Creating indexes...")
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
    
    # New schema: one document per symbol with date-wise nested data
    # Drop old collection to migrate to new schema if duplicates exist
    try:
        symbol_metrics_daily_collection.create_index([('symbol', 1)], name='symbol_idx', unique=True)
    except Exception as idx_err:
        if 'duplicate key' in str(idx_err).lower() or 'E11000' in str(idx_err):
            print(f"⚠️ Found old schema data with duplicates. Migrating to new schema...")
            print(f"⚠️ Dropping old symbol_metrics_daily collection data...")
            symbol_metrics_daily_collection.drop()
            print(f"✓ Collection dropped. Creating fresh indexes...")
            symbol_metrics_daily_collection.create_index([('symbol', 1)], name='symbol_idx', unique=True)
        else:
            raise
    
    symbol_metrics_daily_collection.create_index([('daily_data.date', 1)], name='daily_date_idx')
    nifty_indices_collection.create_index([('symbol', 1)], name='symbol_idx', unique=True)
    print("✅ MongoDB connected successfully")
except Exception as e:
    print(f"⚠️ MongoDB connection failed: {e}")
    import traceback
    traceback.print_exc()
    db = None
    excel_results_collection = None
    bhavcache_collection = None
    symbol_daily_collection = None
    symbol_aggregates_collection = None
    symbol_metrics_collection = None
    symbol_metrics_daily_collection = None
    nifty_indices_collection = None


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
        
        # Store date-wise data in one document per symbol
        # Schema: { symbol: "RELIANCE", company_name: "...", daily_data: [{date: "2026-02-02", impact_cost: ..., free_float_mcap: ...}, ...] }
        if symbol_metrics_daily_collection is not None:
            company_name = row.get('companyName') or row.get('company_name') or ''
            daily_entry = {
                'date': as_on,
                'impact_cost': _safe_float(row.get('impact_cost')),
                'free_float_mcap': _safe_float(row.get('free_float_mcap')),
                'total_market_cap': _safe_float(row.get('total_market_cap')),
                'total_traded_value': _safe_float(row.get('total_traded_value')),
                'source': source,
                'updated_at': datetime.now().isoformat()
            }
            
            # Update or insert the document for this symbol
            # If date already exists in daily_data array, replace it; otherwise add it
            symbol_metrics_daily_collection.update_one(
                {'symbol': symbol},
                {
                    '$set': {
                        'company_name': company_name,
                        'last_updated': datetime.now().isoformat()
                    },
                    '$setOnInsert': {
                        'symbol': symbol,
                        'created_at': datetime.now().isoformat()
                    }
                },
                upsert=True
            )
            
            # Now update the specific date entry in daily_data array
            # Remove existing entry for this date and add new one
            symbol_metrics_daily_collection.update_one(
                {'symbol': symbol},
                {'$pull': {'daily_data': {'date': as_on}}}
            )
            symbol_metrics_daily_collection.update_one(
                {'symbol': symbol},
                {'$push': {'daily_data': daily_entry}}
            )
    except Exception as exc:
        print(f"⚠️ Failed to upsert symbol_metrics for {row.get('symbol')}: {exc}")


def persist_consolidated_results(consolidator, data_type, source='consolidation', skip_daily=False):
    """Store per-symbol per-date values and averages into MongoDB using bulk operations with memory optimization."""
    if consolidator is None or consolidator.df_consolidated is None:
        return
    try:
        from pymongo import UpdateOne
        
        date_cols = [d[0] for d in consolidator.dates_list]
        date_range = None
        if date_cols:
            try:
                start_iso = datetime.strptime(date_cols[0], '%d-%m-%Y').strftime('%Y-%m-%d')
                end_iso = datetime.strptime(date_cols[-1], '%d-%m-%Y').strftime('%Y-%m-%d')
                date_range = {'start': start_iso, 'end': end_iso}
            except Exception:
                date_range = None

        # CHUNKED PROCESSING to avoid OOM
        SYMBOL_BATCH_SIZE = 200 
        total_rows = len(consolidator.df_consolidated)
        
        print(f"[persist] Starting chunked persistence for {total_rows} symbols ({data_type})...")
        
        for i in range(0, total_rows, SYMBOL_BATCH_SIZE):
            batch_df = consolidator.df_consolidated.iloc[i : i + SYMBOL_BATCH_SIZE]
            aggregate_ops = []
            daily_ops = []
            
            for _, row in batch_df.iterrows():
                symbol = str(row.get('Symbol') or '').strip()
                company_name = str(row.get('Company Name') or '').strip()
                if not symbol:
                    continue

                # Prepare aggregate upsert
                avg_val = row.get(consolidator.avg_col)
                days_val = row.get(consolidator.days_col)
                if symbol_aggregates_collection is not None:
                    payload = {
                        'symbol': symbol,
                        'company_name': company_name,
                        'type': data_type,
                        'days_with_data': int(days_val or 0),
                        'average': _safe_float(avg_val),
                        'date_range': date_range,
                        'source': source,
                        'updated_at': datetime.now().isoformat()
                    }
                    if hasattr(consolidator, 'avg_ff_col'):
                        payload['avg_free_float'] = _safe_float(row.get(consolidator.avg_ff_col))
                    aggregate_ops.append(
                        UpdateOne(
                            {'symbol': symbol, 'type': data_type},
                            {'$set': payload},
                            upsert=True
                        )
                    )

                # Prepare daily upserts
                if symbol_daily_collection is not None and not skip_daily:
                    for date_str in date_cols:
                        val = row.get(date_str)
                        if val in (None, ''):
                            continue
                        try:
                            date_iso = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                        except Exception:
                            continue
                        
                        payload = {
                            'symbol': symbol,
                            'company_name': company_name,
                            'date': date_iso,
                            'type': data_type,
                            'value': _safe_float(val),
                            'source': source,
                            'updated_at': datetime.now().isoformat()
                        }
                        daily_ops.append(
                            UpdateOne(
                                {'symbol': symbol, 'date': date_iso, 'type': data_type},
                                {'$set': payload},
                                upsert=True
                            )
                        )

            # Execute bulk operations for this batch immediately
            if aggregate_ops and symbol_aggregates_collection is not None:
                symbol_aggregates_collection.bulk_write(aggregate_ops, ordered=False)
            
            if daily_ops and symbol_daily_collection is not None:
                symbol_daily_collection.bulk_write(daily_ops, ordered=False)
            
            # Explicitly clear lists and batch DF to free memory
            del aggregate_ops
            del daily_ops
            del batch_df
            gc.collect()
            
            processed = min(i + SYMBOL_BATCH_SIZE, total_rows)
            print(f"[persist] ✓ Batched persistence: {processed}/{total_rows} symbols processed")

        print(f"[persist] ✓ All results persisted for {data_type}")
            
    except Exception as exc:
        import traceback
        traceback.print_exc()
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


def get_cached_csv_bulk(date_iso_list, data_type):
    """Bulk fetch cached CSVs for multiple dates (faster than one-by-one)"""
    if bhavcache_collection is None:
        return {}
    
    try:
        # Single query for all dates
        docs = list(bhavcache_collection.find({
            'date': {'$in': date_iso_list},
            'type': data_type
        }))
        
        results = {}
        for doc in docs:
            date_iso = doc.get('date')
            try:
                csv_bytes = doc.get('file_data')
                if csv_bytes:
                    # Determine columns to load based on data_type
                    symbol_col = 'SECURITY' if data_type == 'pr' else 'Symbol'
                    value_col = 'NET_TRDVAL' if data_type == 'pr' else 'Market Cap(Rs.)'
                    name_col = 'SECURITY' if data_type == 'pr' else 'Security Name'

                    req_cols = [symbol_col, value_col]
                    if symbol_col != name_col: # Only add if different from symbol_col
                        req_cols.append(name_col)
                    
                    # OPTIMIZATION: Load only necessary columns and specify dtypes
                    df = pd.read_csv(
                        BytesIO(csv_bytes),
                        usecols=req_cols,
                        dtype={symbol_col: 'category'} # Category saves memory for repeated symbols
                    )
                    
                    results[date_iso] = {
                        'df': df,
                        'records': doc.get('records', len(df)),
                        'columns': doc.get('columns', df.columns.tolist()),
                        'stored_at': doc.get('stored_at')
                    }
            except Exception as e:
                print(f"⚠️ Failed to read cached {data_type} for {date_iso}: {e}")
        
        return results
    except Exception as e:
        print(f"⚠️ Bulk cache fetch failed for {data_type}: {e}")
        return {}


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
    """Build consolidated dataframe - ULTRA-OPTIMIZED with minimal operations."""
    
    if bhavcache_collection is None:
        error_msg = "Database not connected. Check if MONGODB_URI/mongo_uri is correctly set in environment variables."
        if log_fn: log_fn(f"❌ {error_msg}")
        raise ValueError(error_msg)

    # BULK LOAD all dates at once
    cached_dict = get_cached_csv_bulk(date_iso_list, data_type)
    
    frames = []
    missing_dates = []
    for date_iso in date_iso_list:
        cached = cached_dict.get(date_iso)
        if not cached or cached.get('df') is None:
            missing_dates.append(date_iso)
            continue
        df = cached['df']
        df.columns = df.columns.str.strip()
        df['_date_iso'] = date_iso
        frames.append(df)

    if missing_dates and not allow_missing:
        error_msg = f"No cached {data_type.upper()} data for dates: {', '.join(missing_dates)}. Please 'Download Range' for these dates first."
        if log_fn: log_fn(f"❌ {error_msg}")
        raise ValueError(error_msg)

    if not frames:
        error_msg = f"No cached {data_type.upper()} data available for the entire requested range ({date_iso_list[0]} to {date_iso_list[-1]})."
        if log_fn: log_fn(f"❌ {error_msg}")
        raise ValueError(error_msg)

    # Concatenate all data at once (fast)
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

    # OPTIMIZATION: Skip strip() on columns - it's slow. Just convert/clean what matters
    df_all[symbol_col] = df_all[symbol_col].astype(str)
    df_all[name_col] = df_all[name_col].astype(str)
    df_all[value_col] = pd.to_numeric(df_all[value_col], errors='coerce')

    # Remove summary rows (vectorized, no apply())
    symbols_upper = df_all[symbol_col].astype(str).str.upper()
    symbols_normalized = symbols_upper.str.replace(r'[^A-Z0-9]', '', regex=True)
    is_summary = symbols_normalized.isin({'TOTAL', 'LISTED', 'TOTALLISTED', 'LISTEDTOTAL'}) | \
                 symbols_upper.str.startswith(('TOTAL', 'LISTED'))
    df_all = df_all[~is_summary]

    # Filter to allowed symbols if needed
    if allowed_symbols is not None:
        df_all = df_all[df_all[symbol_col].isin(allowed_symbols)]
        if df_all.empty:
            raise ValueError(f"No {data_type.upper()} data for requested symbols")

    # Convert dates once (vectorized)
    df_all['_date_str'] = pd.to_datetime(df_all['_date_iso']).dt.strftime('%d-%m-%Y')
    date_cols = sorted(df_all['_date_str'].unique(), key=lambda d: datetime.strptime(d, '%d-%m-%Y'))

    # ULTRA-FAST RESHAPE: No groupby, just direct unstack with drop_duplicates
    df_all = df_all.drop_duplicates(subset=[symbol_col, '_date_str'], keep='last')
    df_all_pivot = df_all.set_index([symbol_col, '_date_str'])[value_col].unstack(fill_value=None)
    df_all_pivot.reset_index(inplace=True)
    df_all_pivot.rename(columns={symbol_col: 'Symbol'}, inplace=True)

    # Get company names (single pass, handles symbol_col == name_col case)
    if symbol_col == name_col:
        # When symbol and name are the same column, map each to itself
        name_lookup = {sym: sym for sym in df_all[symbol_col].drop_duplicates().values}
    else:
        # Normal case: create mapping from different columns
        unique_df = df_all.drop_duplicates(symbol_col, keep='last')
        name_lookup = unique_df.set_index(symbol_col)[name_col].to_dict()
    df_all_pivot['Company Name'] = df_all_pivot['Symbol'].map(name_lookup)

    # For PR data: Replace company names (in Symbol column) with ticker symbols from MCAP
    if data_type == 'pr' and symbol_name_map:
        # symbol_name_map is CompanyName → TickerSymbol mapping
        # df_all_pivot['Symbol'] currently contains company names (from SECURITY column)
        # Map them to ticker symbols so they match MCAP symbols
        original_companies = df_all_pivot['Symbol'].copy()
        before_count = len(df_all_pivot)
        
        # Create normalized mapping for better matching
        # Normalize: uppercase, remove extra spaces, punctuation
        def normalize_name(name):
            if not name:
                return ''
            return ''.join(c.upper() for c in str(name) if c.isalnum() or c.isspace()).strip()
        
        # Build normalized lookup: normalized_name → ticker_symbol
        normalized_map = {}
        for company_name, ticker in symbol_name_map.items():
            norm_name = normalize_name(company_name)
            if norm_name:
                normalized_map[norm_name] = ticker
        
        # Try exact match first, then normalized match
        def find_ticker(pr_company_name):
            # Try exact match
            if pr_company_name in symbol_name_map:
                return symbol_name_map[pr_company_name]
            # Try normalized match
            norm_pr = normalize_name(pr_company_name)
            return normalized_map.get(norm_pr)
        
        df_all_pivot['Symbol'] = original_companies.apply(find_ticker)
        # Keep original company name
        df_all_pivot['Company Name'] = original_companies
        # Remove rows where mapping failed (no matching MCAP symbol)
        df_all_pivot = df_all_pivot[df_all_pivot['Symbol'].notna()]
        after_count = len(df_all_pivot)
        matched_count = after_count
        unmatched_count = before_count - after_count
        
        if log_fn:
            log_fn(f"✓ PR symbol mapping: {matched_count}/{before_count} matched ({100*matched_count/before_count:.1f}%), {unmatched_count} unmatched")
            if unmatched_count > 0 and unmatched_count <= 10:
                # Show samples of unmatched names
                unmatched_samples = original_companies[~original_companies.isin(df_all_pivot['Company Name'].values)].head(10).tolist()
                log_fn(f"  Unmatched samples: {', '.join(unmatched_samples[:5])}")

    # Get available date columns
    available_cols = [c for c in date_cols if c in df_all_pivot.columns]
    
    # Vectorized metrics (one pass through data)
    df_all_pivot['Days With Data'] = df_all_pivot[available_cols].notna().sum(axis=1)
    df_all_pivot[avg_col] = df_all_pivot[available_cols].mean(axis=1)

    # Remove empty rows
    df_all_pivot = df_all_pivot[~df_all_pivot[available_cols].isna().all(axis=1)].reset_index(drop=True)

    # Sort by average
    df_all_pivot = df_all_pivot.sort_values(by=avg_col, ascending=False, na_position='last').reset_index(drop=True)

    # Final column order
    final_cols = ['Symbol', 'Company Name', 'Days With Data', avg_col] + available_cols
    df_all_pivot = df_all_pivot[final_cols]

    dates_list = [(d, datetime.strptime(d, '%d-%m-%Y')) for d in available_cols]

    return df_all_pivot, dates_list, avg_col

# Configuration
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', './Backend/uploads/market_cap')
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

def calculate_averages_from_consolidated_data(symbols, start_date=None, end_date=None):
    """
    Calculate averages using the same method as consolidation Excel files.
    This ensures consistency between dashboard and consolidation exports.
    """
    if not symbols:
        return {}
    
    try:
        # Build date list the same way as consolidation
        if start_date and end_date:
            try:
                start_dt = date_parser.parse(start_date)
                end_dt = date_parser.parse(end_date)
                if start_dt > end_dt:
                    return {}
                current = start_dt
                date_iso_list = []
                while current <= end_dt:
                    if current.weekday() < 5:  # Only weekdays
                        date_iso_list.append(current.strftime('%Y-%m-%d'))
                    current += timedelta(days=1)
            except:
                return {}
        else:
            # If no date range specified, return empty (same as original behavior)
            return {}
        
        if not date_iso_list:
            return {}
        
        print(f"[calculate_averages_from_consolidated_data] Processing {len(date_iso_list)} dates for {len(symbols)} symbols")
        
        # Use the same consolidation logic for MCAP data
        result = {}
        
        try:
            # Get MCAP averages using consolidation method
            mcap_df, dates_list, avg_col = build_consolidated_from_cache(
                date_iso_list, 'mcap', allow_missing=True, log_fn=None,
                allowed_symbols=set(symbols), symbol_name_map=None
            )
            
            # Extract averages for requested symbols
            for _, row in mcap_df.iterrows():
                symbol = row['Symbol']
                if symbol in symbols:
                    if symbol not in result:
                        result[symbol] = {}
                    result[symbol]['avg_total_market_cap'] = row.get(avg_col)
                    
                    # Calculate free float average if Free Float Market Cap columns exist
                    ff_cols = [col for col in mcap_df.columns if 'free' in col.lower() and 'float' in col.lower()]
                    if ff_cols:
                        # Look for daily free float columns
                        date_cols = [c for c in mcap_df.columns if c not in ['Symbol', 'Company Name', 'Days With Data', avg_col]]
                        # This is tricky - the consolidation doesn't separate free float, so use proportional estimate
                        # For now, use the same as total market cap (this should be improved with actual free float data)
                        result[symbol]['avg_free_float_mcap'] = row.get(avg_col)
            
            print(f"[calculate_averages_from_consolidated_data] Got MCAP averages for {len(result)} symbols")
            
        except Exception as exc:
            print(f"⚠️ Failed to get MCAP averages from consolidated data: {exc}")
        
        try:
            # Get PR averages using consolidation method
            # Create symbol-to-name mapping for PR processing
            mcap_name_map = {}
            if 'mcap_df' in locals():
                mcap_name_map = dict(zip(mcap_df['Symbol'], mcap_df['Company Name']))
            
            pr_name_to_symbol = {v: k for k, v in mcap_name_map.items()} if mcap_name_map else None
            
            pr_df, pr_dates_list, pr_avg_col = build_consolidated_from_cache(
                date_iso_list, 'pr', allow_missing=True, log_fn=None,
                allowed_symbols=None, symbol_name_map=pr_name_to_symbol
            )
            
            # Extract PR averages for requested symbols
            for _, row in pr_df.iterrows():
                symbol = row['Symbol']
                if symbol in symbols:
                    if symbol not in result:
                        result[symbol] = {}
                    result[symbol]['avg_total_traded_value'] = row.get(pr_avg_col)
            
            print(f"[calculate_averages_from_consolidated_data] Got PR averages for {len([s for s in result.values() if 'avg_total_traded_value' in s])} symbols")
            
        except Exception as exc:
            print(f"⚠️ Failed to get PR averages from consolidated data: {exc}")
        
        # Set impact cost to None (not available in consolidation data)
        for symbol_data in result.values():
            if 'avg_impact_cost' not in symbol_data:
                symbol_data['avg_impact_cost'] = None
        
        print(f"[calculate_averages_from_consolidated_data] Final result: averages for {len(result)} symbols")
        return result
        
    except Exception as exc:
        print(f"⚠️ Failed to calculate averages from consolidated data: {exc}")
        return {}


def calculate_averages_from_db(symbols, start_date=None, end_date=None):
    """
    Calculate averages for impact_cost, free_float_mcap, total_market_cap, and total_traded_value
    from the symbol_metrics_daily collection.
    
    DEPRECATED: Use calculate_averages_from_consolidated_data for consistency with consolidation Excel.
    """
    # For backward compatibility, try consolidation method first
    consolidated_averages = calculate_averages_from_consolidated_data(symbols, start_date, end_date)
    if consolidated_averages:
        return consolidated_averages
    
    # Fallback to original DB method with new schema
    if symbol_metrics_daily_collection is None or not symbols:
        return {}
    
    try:
        # New schema: { symbol: "RELIANCE", daily_data: [{date: "2026-02-02", impact_cost: ..., free_float_mcap: ...}, ...] }
        # Build aggregation pipeline to unwind daily_data array and filter by date
        pipeline = [
            {'$match': {'symbol': {'$in': symbols}}},
            {'$unwind': '$daily_data'},
        ]
        
        # Add date filter if provided
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter['$gte'] = start_date
            if end_date:
                date_filter['$lte'] = end_date
            pipeline.append({'$match': {'daily_data.date': date_filter}})
        
        # Calculate averages
        pipeline.extend([
            {
                '$group': {
                    '_id': '$symbol',
                    'avg_impact_cost': {'$avg': '$daily_data.impact_cost'},
                    'avg_free_float_mcap': {'$avg': '$daily_data.free_float_mcap'},
                    'avg_total_market_cap': {'$avg': '$daily_data.total_market_cap'},
                    'avg_total_traded_value': {'$avg': '$daily_data.total_traded_value'},
                    'count': {'$sum': 1}
                }
            }
        ])
        
        result = {}
        for doc in symbol_metrics_daily_collection.aggregate(pipeline):
            symbol = doc['_id']
            result[symbol] = {
                'avg_impact_cost': doc.get('avg_impact_cost'),
                'avg_free_float_mcap': doc.get('avg_free_float_mcap'),
                'avg_total_market_cap': doc.get('avg_total_market_cap'),
                'avg_total_traded_value': doc.get('avg_total_traded_value'),
                'days_count': doc.get('count', 0)
            }
        
        print(f"[calculate_averages_from_db] Calculated averages for {len(result)} symbols from DB (fallback)")
        return result
    except Exception as exc:
        print(f"⚠️ Failed to calculate averages from DB: {exc}")
        return {}


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


def format_dashboard_excel(rows, excel_path, start_date=None, end_date=None):
    """
    Format dashboard Excel with required columns and calculations.
    Uses EXACT SAME averages as consolidation Excel by extracting from consolidation source.
    When date range is provided, replaces dashboard averages with consolidation averages.
    
    Columns:
    - Serial No
    - Symbol
    - Company name
    - Index
    - Avg Impact cost
    - Average Market Cap (EXACT same as consolidation Excel)
    - Average Free Float Market Cap 
    - Average Net Traded Value (EXACT same as consolidation Excel)
    - Day of Listing
    - Broader Index (Nifty 500 if in Nifty 50/Next 50/Midcap 150/Smallcap 250)
    - listed> 6months (Y/N)
    - listed> 1 months (Y/N)
    - Ratio of avg free float to avg total market cap
    - ratio of free float to avg total market cap (current values)
    """
    try:
        df = pd.DataFrame(rows)
        
        # Use EXACT same averages as consolidation Excel by extracting from consolidation source
        # Do NOT use the dashboard data averages - get them from the same place as consolidation Excel
        if start_date and end_date:
            symbols = df['symbol'].tolist() if 'symbol' in df.columns else []
            if symbols:
                print(f"[format_dashboard_excel] *** DEBUG: Extracting EXACT consolidation averages for {len(symbols)} symbols (date range: {start_date} to {end_date})")
                
                # Build date list exactly as consolidation does
                try:
                    start_dt = date_parser.parse(start_date)
                    end_dt = date_parser.parse(end_date)
                    print(f"[format_dashboard_excel] *** DEBUG: Parsed dates - Start: {start_dt}, End: {end_dt}")
                    
                    if start_dt <= end_dt:
                        current = start_dt
                        date_iso_list = []
                        while current <= end_dt:
                            if current.weekday() < 5:  # Only weekdays
                                date_iso_list.append(current.strftime('%Y-%m-%d'))
                            current += timedelta(days=1)
                        
                        print(f"[format_dashboard_excel] *** DEBUG: Created date list: {date_iso_list[:5]}... ({len(date_iso_list)} total dates)")
                        
                        if date_iso_list:
                            consolidation_averages = {}
                            
                            # Get MCAP averages using EXACT same method as consolidation (NO symbol filtering)
                            try:
                                print(f"[format_dashboard_excel] *** DEBUG: Calling build_consolidated_from_cache for MCAP (ALL companies, no filtering)...")
                                mcap_df, dates_list, avg_col = build_consolidated_from_cache(
                                    date_iso_list, 'mcap', allow_missing=True, log_fn=None,
                                    allowed_symbols=None, symbol_name_map=None  # NO FILTERING - get all companies like consolidation
                                )
                                
                                print(f"[format_dashboard_excel] *** DEBUG: Got FULL MCAP consolidation data with {len(mcap_df)} companies (same as consolidation), avg_col={avg_col}")
                                print(f"[format_dashboard_excel] *** DEBUG: Sample MCAP data:")
                                for i, row in mcap_df.head(3).iterrows():
                                    symbol = row['Symbol']
                                    avg_val = row[avg_col]
                                    print(f"[format_dashboard_excel] *** DEBUG:   {symbol}: {avg_val}")
                                
                                # Store MCAP averages for ALL companies, then filter to dashboard symbols
                                mcap_lookup = {}
                                for _, row in mcap_df.iterrows():
                                    symbol = row['Symbol']
                                    mcap_lookup[symbol] = {
                                        'mcap': row[avg_col],
                                        'company_name': row.get('Company Name', '')
                                    }
                                
                                # Filter to only dashboard symbols
                                for symbol in symbols:
                                    if symbol in mcap_lookup:
                                        consolidation_averages[symbol] = mcap_lookup[symbol]
                                
                                print(f"[format_dashboard_excel] *** DEBUG: Stored MCAP averages for {len(consolidation_averages)} dashboard symbols from {len(mcap_lookup)} total companies")
                                
                                # Create symbol-to-name mapping for PR (from FULL data)
                                mcap_name_map = dict(zip(mcap_df['Symbol'], mcap_df['Company Name']))
                                pr_name_to_symbol = {v: k for k, v in mcap_name_map.items()}
                                
                                print(f"[format_dashboard_excel] *** DEBUG: Created PR mapping with {len(pr_name_to_symbol)} entries from FULL consolidation data")
                                
                                # Get PR averages using EXACT same method as consolidation (NO symbol filtering)
                                try:
                                    print(f"[format_dashboard_excel] *** DEBUG: Calling build_consolidated_from_cache for PR (ALL companies, no filtering)...")
                                    pr_df, pr_dates_list, pr_avg_col = build_consolidated_from_cache(
                                        date_iso_list, 'pr', allow_missing=True, log_fn=None,
                                        allowed_symbols=None, symbol_name_map=pr_name_to_symbol  # NO SYMBOL FILTERING - get all like consolidation
                                    )
                                    
                                    print(f"[format_dashboard_excel] *** DEBUG: Got FULL PR consolidation data with {len(pr_df)} companies (same as consolidation), avg_col={pr_avg_col}")
                                    print(f"[format_dashboard_excel] *** DEBUG: Sample PR data:")
                                    for i, row in pr_df.head(3).iterrows():
                                        symbol = row['Symbol']
                                        avg_val = row[pr_avg_col]
                                        print(f"[format_dashboard_excel] *** DEBUG:   {symbol}: {avg_val}")
                                    
                                    # Store PR averages for dashboard symbols only
                                    pr_lookup = {}
                                    for _, row in pr_df.iterrows():
                                        symbol = row['Symbol']
                                        pr_lookup[symbol] = row[pr_avg_col]
                                    
                                    # Add PR data to consolidation_averages for dashboard symbols
                                    for symbol in symbols:
                                        if symbol in pr_lookup and symbol in consolidation_averages:
                                            consolidation_averages[symbol]['traded_value'] = pr_lookup[symbol]
                                    
                                    print(f"[format_dashboard_excel] *** DEBUG: Added PR averages to {len([s for s in consolidation_averages.values() if 'traded_value' in s])} dashboard symbols from {len(pr_lookup)} total companies")
                                    
                                except Exception as exc:
                                    print(f"[format_dashboard_excel] *** ERROR: Could not get PR consolidation averages: {exc}")
                                    import traceback
                                    traceback.print_exc()
                                
                                # Now replace the values in the dataframe
                                print(f"[format_dashboard_excel] *** DEBUG: Replacing values in dataframe...")
                                replacements_made = 0
                                for idx, row in df.iterrows():
                                    symbol = row.get('symbol')
                                    if symbol and symbol in consolidation_averages:
                                        cons_data = consolidation_averages[symbol]
                                        
                                        # Replace market cap
                                        if 'mcap' in cons_data:
                                            old_val = df.at[idx, 'total_market_cap']
                                            new_val = cons_data['mcap']
                                            df.at[idx, 'total_market_cap'] = new_val
                                            print(f"[format_dashboard_excel] *** DEBUG: {symbol} MCAP: {old_val} -> {new_val}")
                                            replacements_made += 1
                                        
                                        # Replace traded value
                                        if 'traded_value' in cons_data:
                                            old_val = df.at[idx, 'total_traded_value']
                                            new_val = cons_data['traded_value']
                                            df.at[idx, 'total_traded_value'] = new_val
                                            print(f"[format_dashboard_excel] *** DEBUG: {symbol} TRADED: {old_val} -> {new_val}")
                                
                                print(f"[format_dashboard_excel] *** DEBUG: Made {replacements_made} replacements")
                                print(f"[format_dashboard_excel] ✅ Updated with EXACT consolidation averages")
                                
                            except Exception as exc:
                                print(f"[format_dashboard_excel] *** ERROR: Could not get MCAP consolidation averages: {exc}")
                                import traceback
                                traceback.print_exc()
                        
                except Exception as exc:
                    print(f"[format_dashboard_excel] *** ERROR: Error processing consolidation data: {exc}")
                    import traceback
                    traceback.print_exc()
        else:
            print(f"[format_dashboard_excel] No date range provided (start_date={start_date}, end_date={end_date}) - using existing dashboard values as-is")
        
        # Map listingDate to listing_date for consistency
        if 'listingDate' in df.columns and 'listing_date' not in df.columns:
            df['listing_date'] = df['listingDate']
        
        # Calculate days since listing
        def calculate_listing_info(listing_date):
            if not listing_date or pd.isna(listing_date):
                return None, 'N', 'N'
            try:
                if isinstance(listing_date, str):
                    list_dt = pd.to_datetime(listing_date)
                else:
                    list_dt = listing_date
                today = datetime.now()
                days_since = (today - list_dt).days
                months_since = days_since / 30.44
                listed_6m = 'Y' if months_since >= 6 else 'N'
                listed_1m = 'Y' if months_since >= 1 else 'N'
                return list_dt.strftime('%Y-%m-%d'), listed_6m, listed_1m
            except:
                return None, 'N', 'N'
        
        # Determine broader index (use DB index if available, fallback to primary_index or API index)
        def get_broader_index(row):
            # Strictly use Index (DB) per user request
            index = row.get('index')
            if not index or pd.isna(index):
                return ''
            index_upper = str(index).upper().replace(' ', '')
            # Only Nifty 50, Next 50, Midcap 150, Smallcap 250 qualify for NIFTY 500 broader label
            qualifying_indices = ['NIFTY50', 'NIFTYNEXT50', 'NIFTYMIDCAP150', 'NIFTYSMALLCAP250']
            for q_idx in qualifying_indices:
                if q_idx == index_upper:
                    return 'NIFTY 500'
            return ''
        
        # Apply calculations
        listing_info = df.apply(lambda row: calculate_listing_info(row.get('listing_date')), axis=1)
        df['Day of Listing'] = listing_info.apply(lambda x: x[0] if x else None)
        df['listed> 6months'] = listing_info.apply(lambda x: x[1] if x else 'N')
        df['listed> 1 months'] = listing_info.apply(lambda x: x[2] if x else 'N')
        
        df['Broader Index'] = df.apply(get_broader_index, axis=1)
        
        # Calculate ratio of avg free float to avg total market cap
        df['Ratio of avg free float to avg total market cap'] = df.apply(
            lambda row: round(row.get('free_float_mcap', 0) / row.get('total_market_cap', 1), 4) 
            if row.get('total_market_cap') and row.get('total_market_cap') > 0 
            else None, 
            axis=1
        )
        
        # Calculate ratio of free float to avg total market cap (current values)
        df['ratio of free float to avg total market cap'] = df.apply(
            lambda row: round(row.get('free_float_mcap', 0) / row.get('total_market_cap', 1), 4) 
            if row.get('total_market_cap') and row.get('total_market_cap') > 0 
            else None, 
            axis=1
        )
        
        # Reorder and rename columns
        output_columns = [
            ('Serial No', 'serial_no'),
            ('Symbol', 'symbol'),
            ('Company name', 'companyName'),
            ('Index (DB)', 'index'),  # Index from Nifty DB
            ('Index (API)', 'index_from_api'),  # Original API index
            ('Avg Impact cost', 'impact_cost'),
            ('Average Market Cap', 'total_market_cap'),
            ('Average Free Float Market Cap', 'free_float_mcap'),
            ('Average Net Traded Value', 'total_traded_value'),
            ('Day of Listing', 'Day of Listing'),
            ('Broader Index', 'Broader Index'),
            ('listed> 6months', 'listed> 6months'),
            ('listed> 1 months', 'listed> 1 months'),
            ('Ratio of avg free float to avg total market cap', 'Ratio of avg free float to avg total market cap'),
            ('ratio of free float to avg total market cap', 'ratio of free float to avg total market cap')
        ]
        
        # Add serial numbers
        df.insert(0, 'serial_no', range(1, len(df) + 1))
        
        # Create output dataframe with renamed columns
        output_df = pd.DataFrame()
        for new_name, old_name in output_columns:
            if old_name in df.columns:
                output_df[new_name] = df[old_name]
            else:
                output_df[new_name] = None
        
        # Save to Excel using openpyxl engine (most compatible)
        output_df.to_excel(excel_path, index=False, sheet_name='Dashboard', engine='openpyxl')
        
        # Apply formatting
        from openpyxl import load_workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
        
        wb = load_workbook(excel_path)
        ws = wb.active
        
        # Define styles
        header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
        header_font = Font(bold=True, color='FFFFFF', size=11)
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Format header row (row 1)
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment
            cell.border = thin_border
        
        # Set column widths and apply number formatting
        column_formats = {
            'A': (8, None),  # Serial No
            'B': (12, None),  # Symbol
            'C': (30, None),  # Company name
            'D': (20, None),  # Index (DB)
            'E': (20, None),  # Index (API)
            'F': (15, numbers.FORMAT_NUMBER_00),  # avg Impact cost
            'G': (20, '#,##0.00'),  # avg total market cap
            'H': (22, '#,##0.00'),  # Avg Free float market cap
            'I': (20, '#,##0.00'),  # Avg daily traded value
            'J': (15, None),  # Day of Listing
            'K': (15, None),  # Broader Index
            'L': (13, None),  # listed> 6months
            'M': (13, None),  # listed> 1 months
            'N': (18, '0.0000'),  # Ratio of avg free float to avg total market cap
            'O': (18, '0.0000'),  # ratio of free float to avg total market cap
        }
        
        # Apply column formatting
        for col_letter, (width, num_format) in column_formats.items():
            ws.column_dimensions[col_letter].width = width
            if num_format:
                for row in range(2, ws.max_row + 1):
                    cell = ws[f'{col_letter}{row}']
                    cell.number_format = num_format
                    cell.border = thin_border
            else:
                for row in range(2, ws.max_row + 1):
                    cell = ws[f'{col_letter}{row}']
                    cell.border = thin_border
        
        # Freeze header row
        ws.freeze_panes = 'A2'
        
        # Save formatted workbook
        wb.save(excel_path)
        
        print(f"✅ Created dashboard Excel with {len(output_df)} rows and formatting")
        return True
    except Exception as e:
        print(f"⚠️ Error formatting dashboard Excel: {e}")
        import traceback
        traceback.print_exc()
        return False



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
                # Retry logic for network failures
                max_retries = 3
                retry_delay = 2  # seconds
                
                for attempt in range(max_retries):
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
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'application/json, text/plain, */*',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Connection': 'keep-alive'
                        }

                        response = requests.get(api_url, params=params, headers=headers, timeout=30)
                        if response.status_code != 200:
                            if response.status_code == 404:
                                # 404 means no data for this date (holiday/weekend)
                                result['errors'].append({
                                    'date': nse_date_formatted,
                                    'error': f'NSE API error: {response.status_code} - No data available (possibly a holiday)'
                                })
                                result['failed_count'] += 1
                                return index, result
                            elif attempt < max_retries - 1:
                                # Retry for other errors
                                time.sleep(retry_delay)
                                continue
                            else:
                                result['errors'].append({
                                    'date': nse_date_formatted,
                                    'error': f'NSE API error: {response.status_code}'
                                })
                                result['failed_count'] += 1
                                return index, result
                        break  # Success, exit retry loop
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                            requests.exceptions.RequestException) as conn_err:
                        if attempt < max_retries - 1:
                            print(f"Connection error for {nse_date_formatted} (attempt {attempt+1}/{max_retries}): {conn_err}")
                            time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            result['errors'].append({
                                'date': nse_date_formatted,
                                'error': f'Connection failed after {max_retries} attempts: {str(conn_err)}'
                            })
                            result['failed_count'] += 1
                            return index, result
                    except Exception as e:
                        result['errors'].append({
                            'date': nse_date_formatted,
                            'error': f'Error fetching ZIP: {str(e)}'
                        })
                        result['failed_count'] += 1
                        return index, result
                
                # If we get here, response is successful
                try:

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
                        'error': f'Error processing ZIP file: {str(e)}'
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

        # Categorize errors for better user feedback
        error_categories = {
            'holidays': [],
            'network': [],
            'other': []
        }
        
        for error in downloads_summary['errors']:
            error_msg = error.get('error', '').lower()
            if '404' in error_msg or 'no data available' in error_msg or 'holiday' in error_msg:
                error_categories['holidays'].append(error['date'])
            elif 'connection' in error_msg or 'resolve' in error_msg or 'getaddrinfo' in error_msg or 'timeout' in error_msg:
                error_categories['network'].append(error['date'])
            else:
                error_categories['other'].append(error)
        
        # Build helpful error summary
        error_summary = []
        if error_categories['holidays']:
            error_summary.append(f"No data available (likely holidays): {', '.join(error_categories['holidays'])}")
        if error_categories['network']:
            error_summary.append(f"Network/connection errors: {', '.join(error_categories['network'])} - Try again later or check internet connection")
        if error_categories['other']:
            error_summary.append(f"{len(error_categories['other'])} other errors - check details below")

        return jsonify({
            'success': True,
            'summary': {
                'total_requested': downloads_summary['total_requested'],
                'cached': downloads_summary['cached_count'],
                'fetched': downloads_summary['fetched_count'],
                'failed': downloads_summary['failed_count'],
                'refresh_mode': refresh_mode,
                'parallel_workers': parallel_workers,
                'error_summary': error_summary
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
    Optimized for Render free tier (30s timeout).
    
    For large requests (>50 symbols), use batch_index parameter to paginate:
    - First call: get total_batches from response
    - Subsequent calls: pass batch_index=0,1,2... to get each batch
    - Frontend accumulates all results
    """
    try:
        data = request.get_json() or {}
        req_id = f"symboldash-{int(time.time() * 1000)}"
        start_time = time.perf_counter()
        
        # Check if this is a batch request or full request
        batch_index = data.get('batch_index')  # None = process all, int = specific batch
        
        date_str = data.get('date')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        save_to_file = data.get('save_to_file', True)
        provided_symbols = data.get('symbols') or []
        top_n = data.get('top_n', 100)  # Default to 100 for Render
        top_n_by = data.get('top_n_by') or 'mcap'
        as_on = datetime.now().strftime('%Y-%m-%d')

        # Render detection and settings
        is_render = os.environ.get('RENDER') == 'true' or os.environ.get('RENDER_SERVICE_NAME')
        

        # Force user requirements: 11 batches of 100 symbols (top 1100 by MCAP)
        BATCH_SIZE = 100
        TOTAL_SYMBOLS = 1100
        MAX_TIME_TOTAL = 55
        MAX_TIME_PER_SYMBOL = 1.0
        MAX_WORKERS = 50

        print(f"[symbol-dashboard][start] id={req_id} batch_index={batch_index} "
              f"is_render={is_render} top_n={top_n}")


        symbols = provided_symbols[:] if provided_symbols else []
        tag = None
        
        # Dashboard Symbol Fetching Logic:
        # 1. If symbols provided in payload, use them.
        # 2. Otherwise, check MongoDB 'symbol_aggregates' for top 1100 by MCAP (Primary).
        # 3. Fallback to local 'nosubject/Market_Cap.xlsx' if DB is empty or not connected.

        if not symbols:
            # 1. Try MongoDB first (Most robust)
            if symbol_aggregates_collection is not None:
                try:
                    # Fetch top 1100 symbols by average market cap
                    db_symbols = list(symbol_aggregates_collection.find(
                        {'type': 'mcap'},
                        {'symbol': 1, 'average': 1}
                    ).sort('average', -1).limit(TOTAL_SYMBOLS))
                    
                    if db_symbols:
                        symbols = [s['symbol'] for s in db_symbols]
                        print(f"[symbol-dashboard] Got {len(symbols)} symbols from MongoDB (top {TOTAL_SYMBOLS} by Avg MCAP)")
                except Exception as e:
                    print(f"[symbol-dashboard][mongodb-error] {e}")

            # 2. Fallback to Excel if DB search failed or returned nothing
            if not symbols:
                market_cap_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nosubject', 'Market_Cap.xlsx')
                if os.path.exists(market_cap_path):
                    try:
                        df = pd.read_excel(market_cap_path)
                        # Try to find the right columns for symbol and market cap
                        symbol_col = None
                        mcap_col = None
                        for c in df.columns:
                            if str(c).strip().lower() in ['symbol', 'symbols']:
                                symbol_col = c
                            if 'mcap' in str(c).strip().lower() or 'market cap' in str(c).strip().lower():
                                mcap_col = c
                        
                        if symbol_col and mcap_col:
                            df = df[[symbol_col, mcap_col]].dropna()
                            df = df.sort_values(by=mcap_col, ascending=False)
                            symbols = df[symbol_col].astype(str).tolist()[:TOTAL_SYMBOLS]
                            print(f"[symbol-dashboard] Got {len(symbols)} symbols from Market_Cap.xlsx (top {TOTAL_SYMBOLS} MCAP)")
                    except Exception as exc:
                        print(f"⚠️ Failed to fetch symbols from Market_Cap.xlsx: {exc}")
                else:
                    print(f"⚠️ Market_Cap.xlsx not found at {market_cap_path}")

        if not symbols:
            error_msg = (
                "No symbols found. The dashboard requires the top 1100 symbols by Market Cap. "
                "Please run a 'Consolidation Export' (with fast_mode=False) first to populate the database "
                "with symbol rankings."
            )
            return jsonify({'error': error_msg}), 400

        symbols = list(dict.fromkeys(symbols))[:TOTAL_SYMBOLS]  # Remove duplicates, force top 1100
        total_symbols = len(symbols)

        # Always 10 batches of 100
        symbol_batches = [symbols[i:i + BATCH_SIZE] for i in range(0, TOTAL_SYMBOLS, BATCH_SIZE)]
        total_batches = len(symbol_batches)

        print(f"[symbol-dashboard] {total_symbols} symbols -> {total_batches} batches of {BATCH_SIZE}")

        # If batch_index specified, only process that batch
        if batch_index is not None:
            batch_idx = int(batch_index)
            if batch_idx < 0 or batch_idx >= total_batches:
                return jsonify({
                    'success': True,
                    'count': 0,
                    'rows': [],
                    'errors': [],
                    'batch_index': batch_idx,
                    'total_batches': total_batches,
                    'total_symbols': total_symbols,
                    'complete': True,
                    'message': f'Invalid batch index {batch_idx}'
                }), 200
            
            # Process single batch
            batch_symbols = symbol_batches[batch_idx]
            print(f"[symbol-dashboard] Processing batch {batch_idx + 1}/{total_batches} ({len(batch_symbols)} symbols)")
            
            # Load aggregates data for this batch
            symbol_pr_data = {}
            symbol_mcap_data = {}
            if symbol_aggregates_collection is not None:
                try:
                    pr_count = 0
                    for doc in symbol_aggregates_collection.find({'symbol': {'$in': batch_symbols}, 'type': 'pr'}):
                        sym = doc.get('symbol')
                        if sym:
                            pr_count += 1
                            symbol_pr_data[sym] = {'days_with_data': doc.get('days_with_data', 0), 'avg_pr': doc.get('average')}
                            if sym not in symbol_mcap_data:
                                symbol_mcap_data[sym] = {}
                            symbol_mcap_data[sym]['total_traded_value'] = doc.get('average')
                    
                    mcap_count = 0
                    for doc in symbol_aggregates_collection.find({'symbol': {'$in': batch_symbols}, 'type': 'mcap'}):
                        sym = doc.get('symbol')
                        if sym:
                            mcap_count += 1
                            if sym not in symbol_mcap_data:
                                symbol_mcap_data[sym] = {}
                            symbol_mcap_data[sym]['avg_mcap'] = doc.get('average')
                    
                    print(f"[symbol-dashboard] Loaded {mcap_count} MCAP + {pr_count} PR averages for batch {batch_idx + 1}")
                except Exception as e:
                    print(f"⚠️ [symbol-dashboard] Error loading aggregates: {e}")
            
            rows, errors = process_symbol_batch(
                batch_symbols, as_on, MAX_WORKERS, MAX_TIME_PER_SYMBOL * len(batch_symbols),
                symbol_pr_data, symbol_mcap_data
            )
            
            # Add index from DB
            index_map = primary_index_map_from_db(batch_symbols)
            
            # Enrich with aggregates data
            if symbol_aggregates_collection is not None:
                try:
                    for doc in symbol_aggregates_collection.find({'symbol': {'$in': batch_symbols}, 'type': 'pr'}):
                        sym = doc.get('symbol')
                        if sym:
                            for row in rows:
                                if row.get('symbol') == sym:
                                    row['days_with_data'] = doc.get('days_with_data', 0)
                                    break
                except Exception as exc:
                    print(f"⚠️ Failed to fetch days_with_data: {exc}")
            
            for row in rows:
                sym = row.get('symbol')
                if sym and sym in index_map:
                    row['primary_index'] = index_map[sym]
            
            # Persist to DB
            for row in rows:
                upsert_symbol_metrics(dict(row), source='symbol_dashboard')
            
            duration = time.perf_counter() - start_time
            is_last_batch = (batch_idx == total_batches - 1)

            # If this is the last batch, generate Excel and provide download_url
            download_url = None
            db_id = None
            download_name = None
            if is_last_batch and len(rows) > 0:
                download_name = f"Symbol_Dashboard_batch_{batch_idx+1}_{len(rows)}.xlsx"
                try:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
                    excel_path = temp_file.name
                    temp_file.close()
                    
                    # Use formatted Excel output
                    format_dashboard_excel(rows, excel_path, start_date=start_date_str, end_date=end_date_str)
                    
                    db_id = save_excel_to_database(excel_path, download_name, {
                        'symbols': len(rows),
                        'batch': batch_idx+1,
                        'as_on': as_on
                    })
                    if db_id:
                        download_url = f"/api/nse-symbol-dashboard/download?id={db_id}"
                    os.remove(excel_path)
                except Exception as exc:
                    print(f"[symbol-dashboard][Excel batch] failed: {exc}")

            response = {
                'success': True,
                'count': len(rows),
                'rows': rows,
                'errors': errors,
                'batch_index': batch_idx,
                'total_batches': total_batches,
                'total_symbols': total_symbols,
                'symbols_in_batch': len(batch_symbols),
                'complete': is_last_batch,
                'duration_seconds': round(duration, 1)
            }
            if is_last_batch:
                response['file'] = download_name
                response['file_id'] = str(db_id) if db_id else None
                response['download_url'] = download_url
                # Only delete Market_Cap.xlsx after the last batch
                market_cap_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nosubject', 'Market_Cap.xlsx')
                if os.path.exists(market_cap_path):
                    try:
                        os.remove(market_cap_path)
                        print(f"✓ Market_Cap.xlsx deleted after all batches processed")
                    except Exception as exc:
                        print(f"⚠️ Could not delete Market_Cap.xlsx: {exc}")
            return jsonify(response), 200

        # === FULL REQUEST: Process all batches with time limit ===
        all_rows = []
        all_errors = []
        batches_completed = 0
        
        # Pre-fetch data from DB (fast)
        index_map = primary_index_map_from_db(symbols)
        
        symbol_pr_data = {}
        symbol_mcap_data = {}
        if symbol_aggregates_collection is not None:
            try:
                pr_count = 0
                for doc in symbol_aggregates_collection.find({'symbol': {'$in': symbols}, 'type': 'pr'}):
                    sym = doc.get('symbol')
                    if sym:
                        pr_count += 1
                        symbol_pr_data[sym] = {'days_with_data': doc.get('days_with_data', 0), 'avg_pr': doc.get('average')}
                        # Also add the PR average (total_traded_value) to symbol_mcap_data
                        if sym not in symbol_mcap_data:
                            symbol_mcap_data[sym] = {}
                        symbol_mcap_data[sym]['total_traded_value'] = doc.get('average')
                print(f"[dashboard] Loaded {pr_count} PR averages from DB")
                
                mcap_count = 0
                for doc in symbol_aggregates_collection.find({'symbol': {'$in': symbols}, 'type': 'mcap'}):
                    sym = doc.get('symbol')
                    if sym:
                        mcap_count += 1
                        if sym not in symbol_mcap_data:
                            symbol_mcap_data[sym] = {}
                        symbol_mcap_data[sym]['avg_mcap'] = doc.get('average')
                        symbol_mcap_data[sym]['avg_free_float'] = doc.get('avg_free_float')
                print(f"[dashboard] Loaded {mcap_count} MCAP averages from DB")
                
                if mcap_count == 0 and pr_count == 0:
                    print("⚠️ [dashboard] No persistent averages found in DB! To use exact consolidation averages, perform a 'Consolidation Export' first (with fast_mode=false).")
                else:
                    # Show a sample of what was loaded
                    sample_sym = next(iter(symbol_mcap_data.keys())) if symbol_mcap_data else None
                    if sample_sym:
                        print(f"[dashboard] Sample data for {sample_sym}: {symbol_mcap_data[sample_sym]}")
            except Exception as e:
                print(f"⚠️ [dashboard] Error loading aggregates from DB: {e}")
                pass

        fetcher = SymbolMetricsFetcher()
        
        for batch_idx, batch_symbols in enumerate(symbol_batches):
            batch_start = time.perf_counter()
            try:
                batch_pr = {s: symbol_pr_data.get(s) for s in batch_symbols if s in symbol_pr_data}
                batch_mcap = {s: symbol_mcap_data.get(s) for s in batch_symbols if s in symbol_mcap_data}
                result = fetcher.build_dashboard(
                    batch_symbols,
                    excel_path=None,
                    max_symbols=None,
                    as_of=as_on,
                    parallel=True,
                    max_workers=MAX_WORKERS,
                    chunk_size=5,
                    symbol_pr_data=batch_pr,
                    symbol_mcap_data=batch_mcap,
                    max_time_seconds=None,
                    fetch_indices_from_csv=False,
                    nifty_indices_collection=nifty_indices_collection
                )
                batch_rows = result.get('rows', [])
                batch_errors = result.get('errors', [])
                
                # Enrich with aggregates data (days_with_data)
                if symbol_aggregates_collection is not None:
                    try:
                        for doc in symbol_aggregates_collection.find({'symbol': {'$in': batch_symbols}, 'type': 'pr'}):
                            sym = doc.get('symbol')
                            if sym:
                                for row in batch_rows:
                                    if row.get('symbol') == sym:
                                        row['days_with_data'] = doc.get('days_with_data', 0)
                                        break
                    except Exception as exc:
                        print(f"⚠️ Failed to fetch days_with_data for batch: {exc}")
                
                # Add primary_index
                for row in batch_rows:
                    sym = row.get('symbol')
                    if sym and sym in index_map:
                        row['primary_index'] = index_map[sym]
                all_rows.extend(batch_rows)
                all_errors.extend(batch_errors)
                batches_completed += 1
                batch_elapsed = time.perf_counter() - batch_start
                print(f"[symbol-dashboard] Batch {batch_idx + 1}/{total_batches}: "
                      f"{len(batch_rows)}/{len(batch_symbols)} in {batch_elapsed:.1f}s "
                      f"(total: {len(all_rows)})")
            except Exception as exc:
                print(f"[symbol-dashboard] Batch {batch_idx + 1} failed: {exc}")
                all_errors.append({'batch': batch_idx + 1, 'error': str(exc)})
                batches_completed += 1
            # Small delay to avoid rate limiting
            if batch_idx < total_batches - 1:
                time.sleep(0.2)

        # Persist results
        for row in all_rows:
            upsert_symbol_metrics(dict(row), source='symbol_dashboard')

        # Generate Excel if requested and we have data
        download_url = None
        db_id = None
        download_name = None
        
        if save_to_file and all_rows:
            download_name = f"Symbol_Dashboard_{tag or 'latest'}_{len(all_rows)}.xlsx"
            try:
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
                excel_path = temp_file.name
                temp_file.close()
                
                # Use formatted Excel output with date range if available
                start_dt = data.get('start_date')
                end_dt = data.get('end_date')
                format_dashboard_excel(all_rows, excel_path, start_date=start_dt, end_date=end_dt)
                
                db_id = save_excel_to_database(excel_path, download_name, {
                    'symbols': len(all_rows),
                    'batches': batches_completed,
                    'as_on': as_on
                })
                
                if db_id:
                    download_url = f"/api/nse-symbol-dashboard/download?id={db_id}"
                
                os.remove(excel_path)
            except Exception as exc:
                print(f"[symbol-dashboard] Excel failed: {exc}")

        duration = time.perf_counter() - start_time
        is_complete = (batches_completed == total_batches)
        
        print(f"[symbol-dashboard][done] id={req_id} rows={len(all_rows)}/{total_symbols} "
              f"batches={batches_completed}/{total_batches} time={duration:.1f}s complete={is_complete}")

        return jsonify({
            'success': True,
            'count': len(all_rows),
            'rows': all_rows,
            'errors': all_errors,
            'file': download_name,
            'file_id': str(db_id) if db_id else None,
            'download_url': download_url,
            'symbols_used': len(all_rows),
            'total_symbols': total_symbols,
            'batches_completed': batches_completed,
            'total_batches': total_batches,
            'complete': is_complete,
            'duration_seconds': round(duration, 1),
            'message': None if is_complete else f'Partial: {len(all_rows)}/{total_symbols} symbols. Use batch_index for remaining.'
        }), 200
        
    except Exception as e:
        print(f"[symbol-dashboard][error] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def process_symbol_batch(symbols, as_on, max_workers, timeout, symbol_pr_data=None, symbol_mcap_data=None):
    """Process a batch of symbols with 10 workers for parallel processing."""
    fetcher = SymbolMetricsFetcher()
    try:
        # Use exactly 10 workers for optimal parallel processing
        effective_workers = 10
        # Larger chunk size so each worker handles more symbols at once
        chunk_size = max(10, len(symbols) // 10)  # Divide work among 10 workers
        print(f"[batch-processing] Fetching {len(symbols)} symbols with {effective_workers} workers, chunk_size={chunk_size}")
        
        # Filter to only the symbols in this batch
        batch_pr_data = {s: symbol_pr_data.get(s) for s in symbols if symbol_pr_data and s in symbol_pr_data} if symbol_pr_data else {}
        batch_mcap_data = {s: symbol_mcap_data.get(s) for s in symbols if symbol_mcap_data and s in symbol_mcap_data} if symbol_mcap_data else {}
        
        result = fetcher.build_dashboard(
            symbols,
            excel_path=None,
            max_symbols=None,
            as_of=as_on,
            parallel=True,
            max_workers=effective_workers,
            chunk_size=chunk_size,
            symbol_pr_data=batch_pr_data,
            symbol_mcap_data=batch_mcap_data,
            max_time_seconds=timeout,
            fetch_indices_from_csv=False,
            nifty_indices_collection=nifty_indices_collection
        )
        return result.get('rows', []), result.get('errors', [])
    except Exception as exc:
        return [], [{'error': str(exc)}]


@app.route('/api/nse-symbol-dashboard/download', methods=['GET'])
def download_symbol_dashboard_file():
    if excel_results_collection is None:
        return jsonify({'error': 'Database not connected'}), 500

    file_id = request.args.get('id')
    if not file_id:
        return jsonify({'error': 'Missing id parameter'}), 400

    try:
        oid = ObjectId(file_id)
    except Exception:
        return jsonify({'error': 'Invalid file id'}), 400

    try:
        doc = excel_results_collection.find_one({'_id': oid})
        if not doc:
            return jsonify({'error': 'File not found'}), 404

        return send_file(
            BytesIO(doc['file_data']),
            mimetype=doc.get('file_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
            as_attachment=True,
            download_name=doc.get('filename', 'Symbol_Dashboard.xlsx')
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/nse-symbol-dashboard/save-excel', methods=['POST'])
def nse_symbol_dashboard_save_excel():
    """
    Accepts all dashboard rows from the frontend and generates a single Excel file for all batches.
    Returns the download_url for the complete file.
    """
    try:
        data = request.get_json() or {}
        rows = data.get('rows', [])
        as_on = data.get('as_on') or datetime.now().strftime('%Y-%m-%d')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not rows or not isinstance(rows, list):
            return jsonify({'error': 'No rows provided'}), 400
            
        download_name = f"Symbol_Dashboard_All_{len(rows)}.xlsx"
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        excel_path = temp_file.name
        temp_file.close()
        
        # Use formatted Excel output with date range
        format_dashboard_excel(rows, excel_path, start_date=start_date, end_date=end_date)
        
        db_id = save_excel_to_database(excel_path, download_name, {
            'symbols': len(rows),
            'as_on': as_on,
            'all_batches': True,
            'start_date': start_date,
            'end_date': end_date
        })
        
        download_url = None
        if db_id:
            download_url = f"/api/nse-symbol-dashboard/download?id={db_id}"
        
        os.remove(excel_path)
        
        return jsonify({
            'success': True,
            'file': download_name,
            'file_id': str(db_id) if db_id else None,
            'download_url': download_url,
            'symbols': len(rows)
        }), 200
    except Exception as e:
        print(f"[symbol-dashboard][save-excel][error] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/consolidate-saved', methods=['POST'])
def consolidate_saved():
    """
    Memory-optimized consolidation of cached NSE CSVs from Mongo into Excel.
    
    Payload (JSON):
    {
        "date": "03-Dec-2025"  # optional, single date
        "start_date": "01-Dec-2025", "end_date": "05-Dec-2025"  # optional range
        "file_type": "both" | "mcap" | "pr"  # default both
        "fast_mode": true/false  # default true, skip DB writes when true
        "optimize_memory": true/false  # default true, use memory optimization
        "max_records_per_batch": 5000  # default 5000, process data in batches
    }

    Response: Excel file (zip when both MCAP and PR are produced).
    """
    work_dir = None  # Initialize early to avoid UnboundLocalError in exception handlers
    req_id = None
    try:
        try:
            payload = request.get_json() or {}
            req_id = f"consolidate-{int(time.time() * 1000)}"
            stage_start = time.perf_counter()
            
            # Memory optimization settings
            optimize_memory = payload.get('optimize_memory', True)
            max_records_per_batch = payload.get('max_records_per_batch', 5000)
            
            # Log initial memory usage
            initial_memory = get_memory_usage_mb()
            print(f"[consolidate-saved][start] id={req_id} initial_memory={initial_memory:.1f}MB optimize_memory={optimize_memory}")
            
            date_str = payload.get('date')
            start_date_str = payload.get('start_date')
            end_date_str = payload.get('end_date')
            file_type = payload.get('file_type', 'both')
            fast_mode = payload.get('fast_mode', False)
            skip_daily = payload.get('skip_daily', True)
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

            # Check memory limits - if too many dates, force batching
            if len(date_iso_list) > 7 and not optimize_memory:
                optimize_memory = True
                print(f"[consolidate-saved][warning] Processing {len(date_iso_list)} dates - forcing memory optimization")

            logs = []
            work_dir = tempfile.mkdtemp()
            results = {}

            def add_log(message):
                logs.append(message)
                print(message)

            def make_consolidator_from_cache_optimized(data_type, allowed_symbols=None, symbol_name_map=None):
                """Memory-optimized consolidation from cache"""
                memory_before = get_memory_usage_mb()
                
                # IMPORTANT: Don't batch the consolidation itself - that causes data loss!
                # Instead, rely on build_consolidated_from_cache's internal optimizations
                # Only for EXTREMELY large date ranges (6+ months), process all at once
                
                add_log(f"Processing {data_type.upper()} for {len(date_iso_list)} dates")
                
                df, dates_list, avg_col = build_consolidated_from_cache(
                    date_iso_list, data_type, allow_missing=allow_missing, log_fn=add_log,
                    allowed_symbols=allowed_symbols, symbol_name_map=symbol_name_map
                )
                
                if df is None or df.empty:
                    raise ValueError(f"No {data_type.upper()} data available for requested dates")
                
                memory_after = get_memory_usage_mb()
                add_log(f"{data_type.upper()} consolidated: {len(df)} companies across {len(dates_list)} dates")
                add_log(f"Memory usage: {memory_before:.1f}MB -> {memory_after:.1f}MB (Δ{memory_after-memory_before:+.1f}MB)")
                
                cons = MarketCapConsolidator(work_dir, file_type=data_type)
                cons.df_consolidated = df
                cons.dates_list = dates_list
                cons.avg_col = avg_col
                cons.days_col = 'Days With Data'
                return cons, len(df), len(dates_list)

            mcap_output_path = None
            pr_output_path = None
            mcap_symbols = None
            mcap_name_map = None

            # Initialize memory optimizer
            optimizer = MemoryOptimizedExporter(compression_level=9)  # Maximum compression

            # Build a label for filenames based on requested dates
            if len(date_iso_list) == 1:
                date_label = date_iso_list[0]
            elif len(date_iso_list) > 1:
                date_label = f"{date_iso_list[0]}_to_{date_iso_list[-1]}"
            else:
                date_label = "dates"
            date_label = date_label.replace('/', '-').replace(' ', '_')

            # Data collection for multi-sheet Excel
            excel_sheets = {}
            
            if file_type in ['mcap', 'both']:
                try:
                    mcap_stage_start = time.perf_counter()
                    consolidator_mcap, companies_count, dates_count = make_consolidator_from_cache_optimized('mcap')
                    
                    add_log(f"Collected MCAP data: {len(consolidator_mcap.df_consolidated)} records")
                    excel_sheets['Market_Cap'] = consolidator_mcap.df_consolidated.copy()
                    
                    mcap_symbols = set(consolidator_mcap.df_consolidated['Symbol'])
                    mcap_name_map = dict(zip(
                        consolidator_mcap.df_consolidated['Symbol'],
                        consolidator_mcap.df_consolidated['Company Name']
                    ))
                    
                    if not fast_mode:
                        persist_start = time.perf_counter()
                        add_log(f"Starting MCAP persistence to Mongo (saving averages, skip_daily={skip_daily})...")
                        # Perform persistence BEFORE deleting the dataframe
                        persist_consolidated_results(consolidator_mcap, 'mcap', source='cached_db', skip_daily=skip_daily)
                        add_log(f"✓ Persisted {companies_count} MCAP averages to DB in {time.perf_counter() - persist_start:.2f}s")
                        persisted = True
                    else:
                        add_log("⚠️ Skipping MCAP DB persistence (fast_mode=True). Averages NOT saved to MongoDB.")
                        persisted = False
                        
                    # Now clear consolidator from memory
                    del consolidator_mcap.df_consolidated
                    del consolidator_mcap
                    gc.collect()
                        
                    results['mcap'] = {
                        'companies': companies_count,
                        'dates': dates_count,
                        'files': len(date_iso_list),
                        'persisted': persisted
                    }
                    add_log(f"MCAP stage done in {time.perf_counter() - mcap_stage_start:.2f}s")
                except ValueError as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': str(exc)}), 400
                except Exception as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': f'Failed to consolidate MCAP: {exc}'}), 500

            if file_type in ['pr', 'both']:
                try:
                    pr_stage_start = time.perf_counter()
                    # IMPORTANT: Reverse the mapping for PR - we need CompanyName → Symbol mapping
                    # so that PR data (which has company names) can be mapped to ticker symbols
                    pr_name_to_symbol = {v: k for k, v in mcap_name_map.items()} if mcap_name_map else None
                    consolidator_pr, companies_count_pr, dates_count_pr = make_consolidator_from_cache_optimized(
                        'pr', allowed_symbols=None, symbol_name_map=pr_name_to_symbol
                    )
                    
                    add_log(f"Collected PR data: {len(consolidator_pr.df_consolidated)} records")
                    excel_sheets['Net_Traded_Value'] = consolidator_pr.df_consolidated.copy()
                    
                    if not fast_mode:
                        persist_start = time.perf_counter()
                        add_log(f"Starting PR persistence to Mongo (saving averages, skip_daily={skip_daily})...")
                        # Perform persistence BEFORE deleting the dataframe
                        persist_consolidated_results(consolidator_pr, 'pr', source='cached_db', skip_daily=skip_daily)
                        add_log(f"✓ Persisted {companies_count_pr} PR averages to DB in {time.perf_counter() - persist_start:.2f}s")
                        persisted_pr = True
                    else:
                        add_log("⚠️ Skipping PR DB persistence (fast_mode=True). Averages NOT saved to MongoDB.")
                        persisted_pr = False

                    # Now clear consolidator from memory
                    del consolidator_pr.df_consolidated
                    del consolidator_pr
                    gc.collect()
                    results['pr'] = {
                        'companies': companies_count_pr,
                        'dates': dates_count_pr,
                        'files': len(date_iso_list),
                        'persisted': persisted_pr
                    }
                    add_log(f"PR stage done in {time.perf_counter() - pr_stage_start:.2f}s")
                except ValueError as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': str(exc)}), 400
                except Exception as exc:
                    shutil.rmtree(work_dir, ignore_errors=True)
                    return jsonify({'error': f'Failed to consolidate PR: {exc}'}), 500

            if not excel_sheets:
                shutil.rmtree(work_dir, ignore_errors=True)
                return jsonify({'error': 'No data collected for Excel creation'}), 500

            # Create single Excel file with multiple sheets
            excel_creation_start = time.perf_counter()
            excel_filename = f"Market_Data_{date_label}.xlsx"
            excel_path = os.path.join(work_dir, excel_filename)
            
            add_log(f"Creating multi-sheet Excel file: {excel_filename}")
            add_log(f"Sheets to create: {list(excel_sheets.keys())}")
            
            if optimize_memory:
                optimizer.create_multi_sheet_excel(excel_sheets, excel_path)
            else:
                # Fallback to openpyxl for multiple sheets
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    for sheet_name, df in excel_sheets.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Get file size and log
            excel_size_mb = os.path.getsize(excel_path) / 1024 / 1024
            add_log(f"✓ Multi-sheet Excel created: {excel_size_mb:.1f}MB in {time.perf_counter() - excel_creation_start:.2f}s")
            
            # Clear sheets data from memory
            del excel_sheets
            gc.collect()

            # Always copy Market_Cap sheet to nosubject/ if MCAP data exists
            try:
                nosubject_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nosubject')
                if not os.path.exists(nosubject_dir):
                    os.makedirs(nosubject_dir)
                if 'mcap' in results:
                    # Copy the multi-sheet file as Market_Cap.xlsx for backward compatibility
                    market_cap_dest = os.path.join(nosubject_dir, 'Market_Cap.xlsx')
                    shutil.copy2(excel_path, market_cap_dest)
                    add_log(f"✓ Multi-sheet Excel copied to {market_cap_dest}")
            except Exception as exc:
                add_log(f"⚠️ Could not copy Excel to nosubject/: {exc}")

            # Send the single Excel file (no ZIP needed!)
            final_memory = get_memory_usage_mb()
            total_elapsed = time.perf_counter() - stage_start
            add_log(f"Export completed: {total_elapsed:.2f}s, Peak memory: {final_memory:.1f}MB, File size: {excel_size_mb:.1f}MB")
            
            response = send_file(
                excel_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=excel_filename
            )
            response.headers['Content-Disposition'] = f"attachment; filename={excel_filename}"
            
            if logs:
                safe_log = ' | '.join(logs)
                safe_log = safe_log.encode('ascii', errors='ignore').decode('ascii')
                response.headers['X-Export-Log'] = safe_log

            print(f"[consolidate-saved][done] id={req_id} sheets={list(results.keys())} elapsed={total_elapsed:.2f}s memory={final_memory:.1f}MB size={excel_size_mb:.1f}MB")

            response.call_on_close(lambda: [shutil.rmtree(work_dir, ignore_errors=True), gc.collect()])
            return response

        except Exception as e:
            if work_dir:
                shutil.rmtree(work_dir, ignore_errors=True)
            error_msg = f"id={req_id} {e}" if req_id else str(e)
            print(f"[consolidate-saved][error] {error_msg}")
            return jsonify({'error': str(e)}), 500

    except Exception as e:
        if work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)
        print(f"[consolidate-saved][error] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nifty-indices/fetch-and-store', methods=['POST'])
def fetch_and_store_nifty_indices():
    """
    Fetch Nifty indices from CSV files and store in MongoDB.
    This should be called when user clicks the 'Refresh Indices' button.
    """
    if nifty_indices_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        print("[fetch-and-store-indices] Starting index fetch from CSV files...")
        fetcher = SymbolMetricsFetcher()
        
        # Fetch indices from CSV with retry logic
        index_mapping = fetcher.fetch_nifty_indices()
        
        if not index_mapping:
            return jsonify({'error': 'Failed to fetch any indices from CSV files'}), 500
        
        # Prepare bulk operations for MongoDB
        bulk_operations = []
        timestamp = datetime.now()
        
        for symbol, indices in index_mapping.items():
            # Standardize symbol for storage
            symbol = str(symbol).strip().upper()
            bulk_operations.append(
                UpdateOne(
                    {'symbol': symbol},
                    {
                        '$set': {
                            'symbol': symbol,
                            'indices': indices,
                            'primary_index': indices[0] if indices else None,
                            'last_updated': timestamp
                        }
                    },
                    upsert=True
                )
            )
        
        # Execute bulk write
        if bulk_operations:
            result = nifty_indices_collection.bulk_write(bulk_operations)
            print(f"[fetch-and-store-indices] ✓ Stored {len(index_mapping)} symbols in DB")
            print(f"[fetch-and-store-indices] Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}")
            
            # Count symbols per index
            index_counts = {}
            for indices in index_mapping.values():
                for idx in indices:
                    index_counts[idx] = index_counts.get(idx, 0) + 1
            
            return jsonify({
                'success': True,
                'total_symbols': len(index_mapping),
                'timestamp': timestamp.isoformat(),
                'index_distribution': index_counts,
                'stats': {
                    'matched': result.matched_count,
                    'modified': result.modified_count,
                    'upserted': result.upserted_count
                }
            })
        else:
            return jsonify({'error': 'No data to store'}), 400
            
    except Exception as e:
        print(f"[fetch-and-store-indices] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/nifty-indices/status', methods=['GET'])
def get_nifty_indices_status():
    """
    Get the status of stored Nifty indices in DB.
    Returns count of symbols and last update timestamp.
    """
    if nifty_indices_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    try:
        total_count = nifty_indices_collection.count_documents({})
        
        # Get last updated timestamp
        latest_doc = nifty_indices_collection.find_one(
            {},
            sort=[('last_updated', -1)]
        )
        
        last_updated = latest_doc.get('last_updated') if latest_doc else None
        
        # Count by index
        pipeline = [
            {'$unwind': '$indices'},
            {'$group': {
                '_id': '$indices',
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}}
        ]
        
        index_distribution = {}
        for doc in nifty_indices_collection.aggregate(pipeline):
            index_distribution[doc['_id']] = doc['count']
        
        return jsonify({
            'total_symbols': total_count,
            'last_updated': last_updated.isoformat() if last_updated else None,
            'index_distribution': index_distribution,
            'has_data': total_count > 0
        })
        
    except Exception as e:
        print(f"[nifty-indices-status] Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nifty-indices/get-symbol-indices', methods=['GET'])
def get_symbol_indices():
    """
    Get indices for specific symbols from DB.
    Query param: symbols (comma-separated)
    """
    if nifty_indices_collection is None:
        return jsonify({'error': 'Database not connected'}), 500
    
    symbols_param = request.args.get('symbols', '')
    if not symbols_param:
        return jsonify({'error': 'Missing symbols parameter'}), 400
    
    try:
        symbols = [s.strip() for s in symbols_param.split(',') if s.strip()]
        
        results = {}
        for doc in nifty_indices_collection.find({'symbol': {'$in': symbols}}):
            results[doc['symbol']] = {
                'indices': doc.get('indices', []),
                'primary_index': doc.get('primary_index'),
                'last_updated': doc.get('last_updated').isoformat() if doc.get('last_updated') else None
            }
        
        return jsonify(results)
        
    except Exception as e:
        print(f"[get-symbol-indices] Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/db-status', methods=['GET'])
def get_db_status():
    """Get database collection counts and status."""
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500
    try:
        status = {}
        for coll_name in db.list_collection_names():
            status[coll_name] = db[coll_name].count_documents({})
        return jsonify({
            'status': 'connected',
            'collections': status,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/db-prune', methods=['POST'])
def prune_db():
    """Prune old database entries to free up space."""
    if db is None:
        return jsonify({'error': 'Database not connected'}), 500
    try:
        data = request.get_json() or {}
        days = int(data.get('days', 60))
        
        # 1. Clear excel_results
        res_excel = db['excel_results'].delete_many({})
        
        # 2. Clear old bhavcache
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_iso = cutoff_date.strftime('%Y-%m-%d')
        res_cache = db['bhavcache'].delete_many({'date': {'$lt': cutoff_iso}})
        
        return jsonify({
            'message': 'Database pruned successfully',
            'deleted_excel_results': res_excel.deleted_count,
            'deleted_old_cache': res_cache.deleted_count,
            'pruned_before': cutoff_iso
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
