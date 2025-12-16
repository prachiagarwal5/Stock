from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import pandas as pd
import glob
import json
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
from io import BytesIO
import zipfile
from dateutil import parser as date_parser
import numpy as np
from pymongo import MongoClient
from bson.binary import Binary
import dotenv
import base64
from google_drive_service import GoogleDriveService
from consolidate_marketcap import MarketCapConsolidator

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
    print("✅ MongoDB connected successfully")
except Exception as e:
    print(f"⚠️ MongoDB connection failed: {e}")
    db = None
    excel_results_collection = None

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

# Configuration
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Session management for scraped CSV files
SCRAPE_SESSIONS = {}  # {session_id: {'folder': path, 'files': [filenames], 'metadata': {...}, 'created_at': datetime}}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_scrape_session():
    """Create a new session for storing scraped CSV files"""
    import uuid
    session_id = str(uuid.uuid4())
    session_folder = os.path.join(tempfile.gettempdir(), f'scrape_session_{session_id}')
    os.makedirs(session_folder, exist_ok=True)
    
    SCRAPE_SESSIONS[session_id] = {
        'folder': session_folder,
        'files': [],
        'metadata': {},
        'created_at': datetime.now()
    }
    
    return session_id

def cleanup_scrape_session(session_id):
    """Clean up temporary files for a session"""
    if session_id in SCRAPE_SESSIONS:
        session = SCRAPE_SESSIONS[session_id]
        folder = session['folder']
        if os.path.exists(folder):
            shutil.rmtree(folder)
        del SCRAPE_SESSIONS[session_id]
        print(f"✅ Cleaned up session: {session_id}")

def cleanup_old_sessions(max_age_hours=24):
    """Clean up sessions older than max_age_hours (default 24 hours)"""
    now = datetime.now()
    sessions_to_delete = []
    
    for session_id, session in SCRAPE_SESSIONS.items():
        age = (now - session['created_at']).total_seconds() / 3600
        if age > max_age_hours:
            sessions_to_delete.append(session_id)
    
    for session_id in sessions_to_delete:
        cleanup_scrape_session(session_id)
        print(f"⏱️ Auto-cleaned old session: {session_id}")
    
    return len(sessions_to_delete)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'message': 'Market Cap Consolidation Service is running'
    }), 200

@app.route('/api/cleanup-old-sessions', methods=['POST'])
def cleanup_old_sessions_endpoint():
    """Clean up scrape sessions older than 24 hours"""
    try:
        cleaned_count = cleanup_old_sessions(max_age_hours=24)
        return jsonify({
            'success': True,
            'sessions_cleaned': cleaned_count,
            'active_sessions': len(SCRAPE_SESSIONS)
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
    Expected payload: {
        "date": "03-Dec-2025",  # Format: DD-Mon-YYYY
        "save_to_file": true/false  # If true, saves to permanent storage; if false, temp stores
    }
    Returns: {
        "success": true,
        "session_id": "...",  # Temp session ID for preview/download
        "file": "mcapDDMMYYYY.csv",
        "date": "03-Dec-2025",
        "records_count": 5000,
        "columns": [...]
    }
    """
    try:
        data = request.get_json()
        nse_date = data.get('date', '')  # Format: "03-Dec-2025"
        save_to_file = data.get('save_to_file', False)
        
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
                        pr_df = pd.read_csv(BytesIO(csv_content))
                        pr_df.columns = pr_df.columns.str.strip()
                        print(f"PR CSV loaded. Columns: {pr_df.columns.tolist()}")
                        print(f"PR Total records: {len(pr_df)}")
                        
                        # Validate columns for pr (uses SECURITY instead of Symbol)
                        if 'SECURITY' not in pr_df.columns:
                            return jsonify({'error': f'SECURITY column not found in PR CSV. Available: {pr_df.columns.tolist()}'}), 400
                    except Exception as e:
                        print(f"Warning: Error reading PR file: {str(e)}")
                        pr_df = None
                
                if save_to_file:
                    # Save to permanent storage
                    output_path_mcap = os.path.join(
                        os.path.dirname(__file__),
                        'nosubject',
                        mcap_filename
                    )
                    os.makedirs(os.path.dirname(output_path_mcap), exist_ok=True)
                    
                    if mcap_df is not None:
                        mcap_df.to_csv(output_path_mcap, index=False)
                        print(f"Saved MCAP to: {output_path_mcap}")
                    
                    output_path_pr = None
                    if pr_df is not None:
                        output_path_pr = os.path.join(
                            os.path.dirname(__file__),
                            'nosubject',
                            pr_filename
                        )
                        pr_df.to_csv(output_path_pr, index=False)
                        print(f"Saved PR to: {output_path_pr}")
                    
                    return jsonify({
                        'success': True,
                        'message': f'Files downloaded and saved',
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
                else:
                    # Create temporary session and store CSVs
                    session_id = create_scrape_session()
                    
                    temp_path_mcap = os.path.join(SCRAPE_SESSIONS[session_id]['folder'], mcap_filename)
                    if mcap_df is not None:
                        mcap_df.to_csv(temp_path_mcap, index=False)
                        SCRAPE_SESSIONS[session_id]['files'].append(mcap_filename)
                    
                    temp_path_pr = None
                    if pr_df is not None:
                        temp_path_pr = os.path.join(SCRAPE_SESSIONS[session_id]['folder'], pr_filename)
                        pr_df.to_csv(temp_path_pr, index=False)
                        SCRAPE_SESSIONS[session_id]['files'].append(pr_filename)
                    
                    SCRAPE_SESSIONS[session_id]['metadata'] = {
                        'date': nse_date_formatted,
                        'type': 'single_download',
                        'has_mcap': mcap_df is not None,
                        'has_pr': pr_df is not None,
                        'mcap_records': len(mcap_df) if mcap_df is not None else 0,
                        'pr_records': len(pr_df) if pr_df is not None else 0
                    }
                    
                    print(f"Stored in temp session {session_id}")
                    
                    return jsonify({
                        'success': True,
                        'session_id': session_id,
                        'files': {
                            'mcap': {
                                'filename': mcap_filename,
                                'records': len(mcap_df) if mcap_df is not None else 0
                            },
                            'pr': {
                                'filename': pr_filename if pr_df is not None else None,
                                'records': len(pr_df) if pr_df is not None else 0
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

# ==================== SCRAPED CSV SESSION ENDPOINTS ====================

@app.route('/api/scrape-session/<session_id>/preview', methods=['GET'])
def preview_scrape_session(session_id):
    """
    Preview data from a scrape session (show first 10 rows of each CSV)
    """
    try:
        if session_id not in SCRAPE_SESSIONS:
            return jsonify({'error': 'Session not found or expired'}), 404
        
        session = SCRAPE_SESSIONS[session_id]
        preview_data = []
        
        # Get preview from each CSV in session
        for filename in session['files']:
            filepath = os.path.join(session['folder'], filename)
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    preview_rows = df.head(10).values.tolist()
                    columns = df.columns.tolist()
                    
                    preview_data.append({
                        'filename': filename,
                        'total_records': len(df),
                        'columns': columns,
                        'preview': preview_rows
                    })
                except Exception as e:
                    preview_data.append({
                        'filename': filename,
                        'error': str(e)
                    })
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'metadata': session['metadata'],
            'file_count': len(session['files']),
            'previews': preview_data
        }), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scrape-session/<session_id>/download-csv', methods=['GET'])
def download_scrape_csv(session_id):
    """
    Download a specific CSV from scrape session
    Query param: filename
    """
    try:
        if session_id not in SCRAPE_SESSIONS:
            return jsonify({'error': 'Session not found or expired'}), 404
        
        filename = request.args.get('filename', '')
        if not filename:
            return jsonify({'error': 'Filename parameter required'}), 400
        
        session = SCRAPE_SESSIONS[session_id]
        
        if filename not in session['files']:
            return jsonify({'error': 'File not found in session'}), 404
        
        filepath = os.path.join(session['folder'], filename)
        
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(
            filepath,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scrape-session/<session_id>/consolidate', methods=['POST'])
def consolidate_scrape_session(session_id):
    """
    Consolidate all CSVs in a scrape session into separate Excel files (one for mcap, one for pr)
    Expected payload: {
        "download_destination": "local" or "google_drive"  # Optional, defaults to local
        "file_type": "mcap" or "pr" or "both"  # Which file to consolidate, default "both"
    }
    """
    global google_drive_service
    
    try:
        if session_id not in SCRAPE_SESSIONS:
            return jsonify({'error': 'Session not found or expired'}), 404
        
        session = SCRAPE_SESSIONS[session_id]
        download_destination = request.json.get('download_destination', 'local') if request.json else 'local'
        file_type = request.json.get('file_type', 'both') if request.json else 'both'
        
        if download_destination not in ['local', 'google_drive']:
            return jsonify({'error': 'Invalid download_destination'}), 400
        
        if file_type not in ['mcap', 'pr', 'both']:
            return jsonify({'error': 'Invalid file_type (mcap, pr, or both)'}), 400
        
        # Separate mcap and pr files
        mcap_files = [f for f in session['files'] if f.startswith('mcap')]
        pr_files = [f for f in session['files'] if f.startswith('pr')]
        
        print(f"\n🔍 Session folder: {session['folder']}")
        print(f"   All files in session: {session['files']}")
        print(f"   MCAP files detected: {mcap_files}")
        print(f"   PR files detected: {pr_files}")
        
        # Verify files actually exist
        for f in session['files']:
            file_path = os.path.join(session['folder'], f)
            exists = os.path.exists(file_path)
            size = os.path.getsize(file_path) if exists else 0
            print(f"   - {f}: exists={exists}, size={size} bytes")
        
        # Create temporary folder for consolidation
        consolidate_folder = tempfile.mkdtemp()
        
        # Copy files to consolidation folder
        for f in session['files']:
            src = os.path.join(session['folder'], f)
            dst = os.path.join(consolidate_folder, f)
            if os.path.exists(src):
                shutil.copy2(src, dst)
        
        results = {
            'success': True,
            'files': {},
            'downloads': []
        }
        
        print(f"\n🔍 Consolidating session {session_id}")
        print(f"   MCAP files found: {len(mcap_files)} - {mcap_files}")
        print(f"   PR files found: {len(pr_files)} - {pr_files}")
        print(f"   File type requested: {file_type}")
        
        # Process MCAP files
        if file_type in ['mcap', 'both'] and mcap_files:
            try:
                # Create temporary folder with only mcap files
                mcap_temp_folder = tempfile.mkdtemp()
                for f in mcap_files:
                    src = os.path.join(session['folder'], f)
                    dst = os.path.join(mcap_temp_folder, f)
                    if os.path.exists(src):
                        shutil.copy2(src, dst)
                
                # Consolidate mcap data
                consolidator_mcap = MarketCapConsolidator(mcap_temp_folder, file_type='mcap')
                companies_count, dates_count = consolidator_mcap.load_and_consolidate_data()
                
                # Create output file
                mcap_output_path = os.path.join(session['folder'], 'Market_Cap.xlsx')
                consolidator_mcap.format_excel_output(mcap_output_path)
                
                # Prepare metadata
                mcap_metadata = {
                    'companies_count': companies_count,
                    'dates_count': dates_count,
                    'files_consolidated': len(mcap_files),
                    'source': 'scrape_session',
                    'data_type': 'market_cap',
                    'consolidated_at': datetime.now().isoformat()
                }
                
                results['files']['mcap'] = {
                    'filename': 'Market_Cap.xlsx',
                    'companies': companies_count,
                    'dates': dates_count
                }
                
                # Handle download for MCAP
                if download_destination == 'google_drive':
                    try:
                        if google_drive_service is None or not google_drive_service.is_authenticated():
                            return jsonify({'error': 'Google Drive not authenticated'}), 401
                        
                        file_id = google_drive_service.upload_file(
                            mcap_output_path,
                            'Market_Cap.xlsx',
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )
                        
                        web_link = google_drive_service.get_file_link(file_id)
                        excel_file_id = save_excel_to_database(mcap_output_path, 'Market_Cap.xlsx', mcap_metadata)
                        
                        results['downloads'].append({
                            'type': 'mcap',
                            'file_name': 'Market_Cap.xlsx',
                            'file_id': file_id,
                            'web_link': web_link,
                            'database_id': str(excel_file_id)
                        })
                    except Exception as e:
                        results['files']['mcap']['error'] = str(e)
                
                # Cleanup temp mcap folder
                shutil.rmtree(mcap_temp_folder, ignore_errors=True)
                print(f"✅ MCAP file created: {mcap_output_path}")
                
            except Exception as e:
                print(f"❌ Error processing MCAP: {str(e)}")
                results['files']['mcap'] = {'error': str(e)}
        
        # Process PR files
        if file_type in ['pr', 'both'] and pr_files:
            print(f"\n📊 Processing PR files...")
            try:
                # Create temporary folder with only pr files
                pr_temp_folder = tempfile.mkdtemp()
                for f in pr_files:
                    src = os.path.join(session['folder'], f)
                    dst = os.path.join(pr_temp_folder, f)
                    if os.path.exists(src):
                        shutil.copy2(src, dst)
                
                # Consolidate pr data (NET_TRDVAL)
                consolidator_pr = MarketCapConsolidator(pr_temp_folder, file_type='pr')
                companies_count_pr, dates_count_pr = consolidator_pr.load_and_consolidate_data()
                
                # Create output file
                pr_output_path = os.path.join(session['folder'], 'Net_Traded_Value.xlsx')
                consolidator_pr.format_excel_output(pr_output_path)
                
                # Prepare metadata
                pr_metadata = {
                    'companies_count': companies_count_pr,
                    'dates_count': dates_count_pr,
                    'files_consolidated': len(pr_files),
                    'source': 'scrape_session',
                    'data_type': 'net_traded_value',
                    'consolidated_at': datetime.now().isoformat()
                }
                
                results['files']['pr'] = {
                    'filename': 'Net_Traded_Value.xlsx',
                    'companies': companies_count_pr,
                    'dates': dates_count_pr
                }
                
                # Handle download for PR
                if download_destination == 'google_drive':
                    try:
                        if google_drive_service is None or not google_drive_service.is_authenticated():
                            return jsonify({'error': 'Google Drive not authenticated'}), 401
                        
                        file_id = google_drive_service.upload_file(
                            pr_output_path,
                            'Net_Traded_Value.xlsx',
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )
                        
                        web_link = google_drive_service.get_file_link(file_id)
                        excel_file_id = save_excel_to_database(pr_output_path, 'Net_Traded_Value.xlsx', pr_metadata)
                        
                        results['downloads'].append({
                            'type': 'pr',
                            'file_name': 'Net_Traded_Value.xlsx',
                            'file_id': file_id,
                            'web_link': web_link,
                            'database_id': str(excel_file_id)
                        })
                    except Exception as e:
                        results['files']['pr']['error'] = str(e)
                
                # Cleanup temp pr folder
                shutil.rmtree(pr_temp_folder, ignore_errors=True)
                print(f"✅ PR file created: {pr_output_path}")
                
            except Exception as e:
                print(f"❌ Error processing PR: {str(e)}")
                results['files']['pr'] = {'error': str(e)}
        
        # Handle local download (create zip if both files exist)
        if download_destination == 'local':
            mcap_output_path = os.path.join(session['folder'], 'Market_Cap.xlsx')
            pr_output_path = os.path.join(session['folder'], 'Net_Traded_Value.xlsx')
            
            print(f"\n📦 Local download mode")
            print(f"   MCAP file exists: {os.path.exists(mcap_output_path)}")
            print(f"   PR file exists: {os.path.exists(pr_output_path)}")
            
            # If both files exist, create a zip
            if os.path.exists(mcap_output_path) and os.path.exists(pr_output_path):
                import zipfile as zf
                zip_path = os.path.join(session['folder'], 'Market_Data.zip')
                with zf.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(mcap_output_path, arcname='Market_Cap.xlsx')
                    zipf.write(pr_output_path, arcname='Net_Traded_Value.xlsx')
                
                # Save both to MongoDB
                save_excel_to_database(mcap_output_path, 'Market_Cap.xlsx', {'type': 'market_cap'})
                save_excel_to_database(pr_output_path, 'Net_Traded_Value.xlsx', {'type': 'net_traded_value'})
                
                # Return zip file
                response = send_file(
                    zip_path,
                    mimetype='application/zip',
                    as_attachment=True,
                    download_name='Market_Data.zip'
                )
                cleanup_scrape_session(session_id)
                shutil.rmtree(consolidate_folder, ignore_errors=True)
                return response, 200
            
            # If only mcap file exists
            elif os.path.exists(mcap_output_path):
                save_excel_to_database(mcap_output_path, 'Market_Cap.xlsx', {'type': 'market_cap'})
                response = send_file(
                    mcap_output_path,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name='Market_Cap.xlsx'
                )
                cleanup_scrape_session(session_id)
                shutil.rmtree(consolidate_folder, ignore_errors=True)
                return response, 200
            
            # If only pr file exists
            elif os.path.exists(pr_output_path):
                save_excel_to_database(pr_output_path, 'Net_Traded_Value.xlsx', {'type': 'net_traded_value'})
                response = send_file(
                    pr_output_path,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    as_attachment=True,
                    download_name='Net_Traded_Value.xlsx'
                )
                cleanup_scrape_session(session_id)
                shutil.rmtree(consolidate_folder, ignore_errors=True)
                return response, 200
        
        # Cleanup after successful export
        cleanup_scrape_session(session_id)
        shutil.rmtree(consolidate_folder, ignore_errors=True)
        
        return jsonify(results), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/scrape-session/<session_id>/cleanup', methods=['POST'])
def cleanup_session(session_id):
    """
    Manually cleanup/delete a scrape session
    """
    try:
        if session_id not in SCRAPE_SESSIONS:
            return jsonify({'error': 'Session not found'}), 404
        
        cleanup_scrape_session(session_id)
        
        return jsonify({
            'success': True,
            'message': f'Session {session_id} cleaned up'
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
        "save_to_file": true/false    # If false, temp stores; if true, saves permanently
    }
    Returns: {
        "success": true,
        "session_id": "...",  # If save_to_file is false
        "summary": {...},
        "files": [...]
    }
    """
    try:
        data = request.get_json()
        start_date_str = data.get('start_date', '')
        end_date_str = data.get('end_date', '')
        save_to_file = data.get('save_to_file', True)
        
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
        
        # Create temporary session if not saving to file
        session_id = None
        if not save_to_file:
            session_id = create_scrape_session()
        
        # Download data for each trading date
        downloads_summary = {
            'success_count': 0,
            'failed_count': 0,
            'files': [],
            'errors': []
        }
        
        print(f"Downloading NSE data for {len(trading_dates)} trading days...")
        
        for trade_date in trading_dates:
            try:
                nse_date_formatted = trade_date.strftime('%d-%b-%Y')
                filename_date = trade_date.strftime('%d%m%Y')
                
                # NSE API request
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
                
                # Download the ZIP file
                response = requests.get(api_url, params=params, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    downloads_summary['errors'].append({
                        'date': nse_date_formatted,
                        'error': f'NSE API error: {response.status_code}'
                    })
                    downloads_summary['failed_count'] += 1
                    continue
                
                # Extract ZIP file
                try:
                    zip_data = BytesIO(response.content)
                    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                        file_list = zip_ref.namelist()
                        
                        # Find market cap file
                        csv_file = None
                        for file in file_list:
                            if file.lower().endswith('.csv') and 'mcap' in file.lower():
                                csv_file = file
                                break
                        
                        if not csv_file:
                            downloads_summary['errors'].append({
                                'date': nse_date_formatted,
                                'error': 'No market cap CSV file found in ZIP'
                            })
                            downloads_summary['failed_count'] += 1
                            continue
                        
                        # Extract CSV content
                        csv_content = zip_ref.read(csv_file)
                        df = pd.read_csv(BytesIO(csv_content))
                        
                        # Create output filename
                        output_filename = f"mcap{filename_date}.csv"
                        
                        # Save to file
                        if save_to_file:
                            output_path = os.path.join(
                                os.path.dirname(__file__),
                                'nosubject',
                                output_filename
                            )
                            os.makedirs(os.path.dirname(output_path), exist_ok=True)
                            df.to_csv(output_path, index=False)
                            
                            downloads_summary['files'].append({
                                'date': nse_date_formatted,
                                'filename': output_filename,
                                'records': len(df),
                                'status': 'saved'
                            })
                        else:
                            # Save to temporary session
                            temp_path = os.path.join(SCRAPE_SESSIONS[session_id]['folder'], output_filename)
                            df.to_csv(temp_path, index=False)
                            SCRAPE_SESSIONS[session_id]['files'].append(output_filename)
                            
                            downloads_summary['files'].append({
                                'date': nse_date_formatted,
                                'filename': output_filename,
                                'records': len(df),
                                'status': 'temp'
                            })
                        
                        downloads_summary['success_count'] += 1
                        print(f"✅ Downloaded: {nse_date_formatted} ({len(df)} records)")
                
                except Exception as e:
                    downloads_summary['errors'].append({
                        'date': nse_date_formatted,
                        'error': f'Error extracting CSV: {str(e)}'
                    })
                    downloads_summary['failed_count'] += 1
            
            except Exception as e:
                downloads_summary['errors'].append({
                    'date': nse_date_formatted,
                    'error': str(e)
                })
                downloads_summary['failed_count'] += 1
        
        # Update session metadata if temp storage
        if session_id:
            SCRAPE_SESSIONS[session_id]['metadata'] = {
                'start_date': start_date_str,
                'end_date': end_date_str,
                'type': 'range_download',
                'total_files': downloads_summary['success_count']
            }
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'summary': {
                'total_requested': len(trading_dates),
                'successful': downloads_summary['success_count'],
                'failed': downloads_summary['failed_count']
            },
            'files': downloads_summary['files'],
            'errors': downloads_summary['errors']
        }), 200
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

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
