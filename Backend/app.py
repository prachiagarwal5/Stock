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
        symbol_aggregates_collection.update_one(
            {'symbol': symbol, 'type': data_type, 'date_range': date_range},
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
    Always saves files to nosubject and returns metadata (no sessions).
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
                
                # Always save to permanent storage
                output_dir = os.path.join(os.path.dirname(__file__), 'nosubject')
                os.makedirs(output_dir, exist_ok=True)

                output_path_mcap = os.path.join(output_dir, mcap_filename)
                if mcap_df is not None:
                    mcap_df.to_csv(output_path_mcap, index=False)
                    print(f"Saved MCAP to: {output_path_mcap}")
                
                output_path_pr = None
                if pr_df is not None:
                    output_path_pr = os.path.join(output_dir, pr_filename)
                    pr_df.to_csv(output_path_pr, index=False)
                    print(f"Saved PR to: {output_path_pr}")
                
                return jsonify({
                    'success': True,
                    'message': 'Files downloaded and saved',
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
    Returns summary and entries (no sessions; files saved to nosubject).
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

            def _write_df(df, fname):
                if df is None:
                    return False
                output_path = os.path.join(os.path.dirname(__file__), 'nosubject', fname)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                df.to_csv(output_path, index=False)
                return True

            if mcap_df is not None:
                fname_mcap = f"mcap{filename_date}.csv"
                _write_df(mcap_df, fname_mcap)
                status = 'cached' if cached_mcap and refresh_mode != 'force' else 'fetched'
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'mcap',
                    'status': status,
                    'filename': fname_mcap,
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
                fname_pr = f"pr{filename_date}.csv"
                _write_df(pr_df, fname_pr)
                status = 'cached' if cached_pr and refresh_mode != 'force' else 'fetched'
                result['entries'].append({
                    'date': nse_date_formatted,
                    'type': 'pr',
                    'status': status,
                    'filename': fname_pr,
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
    Build a dashboard of impact cost, free float market cap, traded value, and index for symbols in MCAP files.
    Payload options:
    {
        "date": "03-Dec-2025" # use persisted nosubject/mcapDDMMYYYY.csv
        "start_date": "01-Dec-2025", "end_date": "05-Dec-2025" # optional range scan in nosubject
        "symbols": ["ABB", ...],  # optional explicit list
        "save_to_file": true/false  # default true, saves Excel to same folder
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
        output_dir = os.path.join(os.path.dirname(__file__), 'nosubject')
        tag = None

        if date_str:
            try:
                date_obj = date_parser.parse(date_str)
            except Exception:
                return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY'}), 400
            filename_date = date_obj.strftime('%d%m%Y')
            tag = filename_date
            mcap_path = os.path.join(output_dir, f"mcap{filename_date}.csv")
            target_files = [mcap_path]

        elif start_date_str and end_date_str:
            try:
                start_dt = date_parser.parse(start_date_str)
                end_dt = date_parser.parse(end_date_str)
            except Exception:
                return jsonify({'error': 'Invalid start/end date format. Use DD-Mon-YYYY'}), 400
            if start_dt > end_dt:
                return jsonify({'error': 'start_date cannot be after end_date'}), 400
            target_files = find_mcap_files_in_range(output_dir, start_dt, end_dt)
            tag = f"{start_dt.strftime('%d%m%Y')}_{end_dt.strftime('%d%m%Y')}"

        if not symbols:
            symbols = collect_symbols_from_files(target_files)

        # Optionally limit to top N by averages from Mongo aggregates
        if symbol_aggregates_collection is not None:
            try:
                top_n_val = int(top_n) if str(top_n).strip() != '' else 1000
            except Exception:
                top_n_val = 1000
            if top_n_val and top_n_val > 0:
                try:
                    agg_docs = list(symbol_aggregates_collection.find({'type': top_n_by}).sort('average', -1).limit(top_n_val))
                    agg_symbols = [doc.get('symbol') for doc in agg_docs if doc.get('symbol')]
                    if agg_symbols:
                        symbols = agg_symbols
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
        if save_to_file:
            os.makedirs(output_dir, exist_ok=True)
            download_name = f"Symbol_Dashboard_{tag or 'latest'}.xlsx"
            excel_path = os.path.join(output_dir, download_name)

        print(f"[symbol-dashboard] symbols_total={total_symbols} slice={len(symbols_slice)} top_n={top_n} by={top_n_by} workers={parallel_workers} chunk={chunk_size}")

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
            chunk_size=max(1, chunk_val)
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
            download_url = f"/api/nse-symbol-dashboard/download?file={download_name}"

        return jsonify({
            'success': True,
            'count': result.get('count', 0),
            'averages': result.get('averages', {}),
            'errors': result.get('errors', []),
            'file': download_name,
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
    file_name = request.args.get('file')

    if not file_name:
        return jsonify({'error': 'file parameter is required'}), 400

    safe_name = secure_filename(file_name)

    base_dir = os.path.join(os.path.dirname(__file__), 'nosubject')

    file_path = os.path.join(base_dir, safe_name)

    if not os.path.exists(file_path):
        return jsonify({'error': 'Dashboard file not found'}), 404

    return send_file(
        file_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=safe_name
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

        # Optional persistent log to disk for history
        try:
            output_dir = os.path.join(os.path.dirname(__file__), 'nosubject')
            os.makedirs(output_dir, exist_ok=True)
            file_path = os.path.join(output_dir, file_name)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
        except Exception as log_exc:
            print(f"⚠️ Failed to persist indices CSV: {log_exc}")

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
    Consolidate saved NSE CSVs from Backend/nosubject into Excel.

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
        payload = request.get_json() or {}
        date_str = payload.get('date')
        start_date_str = payload.get('start_date')
        end_date_str = payload.get('end_date')
        file_type = payload.get('file_type', 'both')
        # Default to persistence on (fast_mode=False) so aggregates for MCAP/PR get stored
        fast_mode = payload.get('fast_mode', False)

        if file_type not in ['mcap', 'pr', 'both']:
            return jsonify({'error': 'Invalid file_type (mcap, pr, or both)'}), 400

        # Parse date filters
        date_dt = None
        start_dt = None
        end_dt = None
        try:
            if date_str:
                date_dt = date_parser.parse(date_str)
            if start_date_str and end_date_str:
                start_dt = date_parser.parse(start_date_str)
                end_dt = date_parser.parse(end_date_str)
                if start_dt > end_dt:
                    return jsonify({'error': 'start_date cannot be after end_date'}), 400
        except Exception:
            return jsonify({'error': 'Invalid date format. Use DD-Mon-YYYY (e.g., 03-Dec-2025)'}), 400

        base_dir = os.path.join(os.path.dirname(__file__), 'nosubject')
        os.makedirs(base_dir, exist_ok=True)

        def _file_dt(path):
            name = os.path.basename(path)
            match = re.search(r'(?:mcap|pr)(\d{2})(\d{2})(\d{4})', name, re.IGNORECASE)
            if not match:
                return None
            day, month, year = match.groups()
            try:
                return datetime(int(year), int(month), int(day))
            except Exception:
                return None

        def _in_scope(path):
            dt = _file_dt(path)
            if dt is None:
                return False
            if date_dt and dt.date() != date_dt.date():
                return False
            if start_dt and end_dt and not (start_dt.date() <= dt.date() <= end_dt.date()):
                return False
            return True

        mcap_files = [p for p in glob.glob(os.path.join(base_dir, 'mcap*.csv')) if _in_scope(p)]
        pr_files = [p for p in glob.glob(os.path.join(base_dir, 'pr*.csv')) if _in_scope(p)]

        if file_type in ['mcap', 'both'] and not mcap_files:
            return jsonify({'error': 'No MCAP CSV files found for the selected date(s)'}), 404
        if file_type in ['pr', 'both'] and not pr_files:
            return jsonify({'error': 'No PR CSV files found for the selected date(s)'}), 404

        work_dir = tempfile.mkdtemp()

        def _copy_selected(paths):
            for src in paths:
                dst = os.path.join(work_dir, os.path.basename(src))
                shutil.copy2(src, dst)

        if file_type in ['mcap', 'both']:
            _copy_selected(mcap_files)
        if file_type in ['pr', 'both']:
            _copy_selected(pr_files)

        results = {}

        # Process MCAP
        mcap_output_path = None
        if file_type in ['mcap', 'both'] and mcap_files:
            try:
                consolidator_mcap = MarketCapConsolidator(work_dir, file_type='mcap')
                companies_count, dates_count = consolidator_mcap.load_and_consolidate_data()
                mcap_output_path = os.path.join(work_dir, 'Market_Cap.xlsx')
                consolidator_mcap.format_excel_output(mcap_output_path)
                if not fast_mode:
                    persist_consolidated_results(consolidator_mcap, 'mcap', source='saved_files')
                results['mcap'] = {
                    'companies': companies_count,
                    'dates': dates_count,
                    'files': len(mcap_files)
                }
                # keep a copy in nosubject for future reuse
                shutil.copy2(mcap_output_path, os.path.join(base_dir, 'Market_Cap.xlsx'))
            except Exception as exc:
                shutil.rmtree(work_dir, ignore_errors=True)
                return jsonify({'error': f'Failed to consolidate MCAP: {exc}'}), 500

        # Process PR
        pr_output_path = None
        if file_type in ['pr', 'both'] and pr_files:
            try:
                consolidator_pr = MarketCapConsolidator(work_dir, file_type='pr')
                companies_count_pr, dates_count_pr = consolidator_pr.load_and_consolidate_data()
                pr_output_path = os.path.join(work_dir, 'Net_Traded_Value.xlsx')
                consolidator_pr.format_excel_output(pr_output_path)
                if not fast_mode:
                    persist_consolidated_results(consolidator_pr, 'pr', source='saved_files')
                results['pr'] = {
                    'companies': companies_count_pr,
                    'dates': dates_count_pr,
                    'files': len(pr_files)
                }
                shutil.copy2(pr_output_path, os.path.join(base_dir, 'Net_Traded_Value.xlsx'))
            except Exception as exc:
                shutil.rmtree(work_dir, ignore_errors=True)
                return jsonify({'error': f'Failed to consolidate PR: {exc}'}), 500

        # Prepare response
        response = None
        if mcap_output_path and pr_output_path:
            zip_path = os.path.join(work_dir, 'Market_Data.zip')
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                zipf.write(mcap_output_path, arcname='Market_Cap.xlsx')
                zipf.write(pr_output_path, arcname='Net_Traded_Value.xlsx')
            response = send_file(
                zip_path,
                mimetype='application/zip',
                as_attachment=True,
                download_name='Market_Data.zip'
            )
        elif mcap_output_path:
            response = send_file(
                mcap_output_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='Market_Cap.xlsx'
            )
        elif pr_output_path:
            response = send_file(
                pr_output_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='Net_Traded_Value.xlsx'
            )
        else:
            shutil.rmtree(work_dir, ignore_errors=True)
            return jsonify({'error': 'No output generated'}), 500

        # ensure temp cleanup after response is sent
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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
