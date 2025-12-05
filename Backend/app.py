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

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = tempfile.mkdtemp()
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

class MarketCapConsolidator:
    def __init__(self, upload_folder, corporate_actions=None):
        self.data_folder = upload_folder
        self.df_consolidated = None
        self.dates_list = []
        self.corporate_actions = corporate_actions or {
            "splits": [],
            "name_changes": [],
            "delistings": []
        }
    
    def _extract_date_from_filename(self, filename):
        """Extract date from filename like mcap10112025.csv"""
        match = re.search(r'mcap(\d{8})', filename)
        if match:
            date_str = match.group(1)
            # Format: DDMMYYYY
            day = date_str[0:2]
            month = date_str[2:4]
            year = date_str[4:8]
            return f"{day}-{month}-{year}"
        return None
    
    def _parse_date_string(self, date_str):
        """Parse date string like '10-11-2025' to datetime"""
        try:
            return datetime.strptime(date_str, "%d-%m-%Y")
        except:
            return None
    
    def load_and_consolidate_data(self):
        """Load all CSV files and consolidate data"""
        csv_files = sorted(glob.glob(os.path.join(self.data_folder, 'mcap*.csv')))
        
        if not csv_files:
            raise Exception(f"No CSV files found in {self.data_folder}")
        
        # Dictionary to store data: {symbol: {date: market_cap}}
        consolidated_data = {}
        company_names = {}  # Store company names
        self.dates_list = []
        
        for csv_file in csv_files:
            date_str = self._extract_date_from_filename(os.path.basename(csv_file))
            if not date_str:
                continue
            
            self.dates_list.append((date_str, self._parse_date_string(date_str)))
            
            # Read CSV
            try:
                df = pd.read_csv(csv_file)
                # Strip whitespace from column names
                df.columns = df.columns.str.strip()
            except Exception as e:
                raise Exception(f"Error reading {csv_file}: {e}")
            
            # Extract Symbol and Market Cap
            for idx, row in df.iterrows():
                symbol = str(row.get('Symbol', '')).strip()
                market_cap = row.get('Market Cap(Rs.)', '')
                security_name = str(row.get('Security Name', '')).strip()
                
                if symbol and market_cap:
                    if symbol not in consolidated_data:
                        consolidated_data[symbol] = {}
                    
                    # Convert market cap to float
                    try:
                        market_cap_float = float(market_cap)
                        consolidated_data[symbol][date_str] = market_cap_float
                    except:
                        consolidated_data[symbol][date_str] = None
                    
                    company_names[symbol] = security_name
        
        # Sort dates
        self.dates_list.sort(key=lambda x: x[1])
        sorted_dates = [d[0] for d in self.dates_list]
        
        # Create consolidated dataframe
        data_for_df = []
        for symbol in sorted(consolidated_data.keys()):
            row = {
                'Symbol': symbol,
                'Company Name': company_names.get(symbol, symbol)
            }
            
            # Add market cap for each date
            for date_str in sorted_dates:
                row[date_str] = consolidated_data[symbol].get(date_str, None)
            
            data_for_df.append(row)
        
        self.df_consolidated = pd.DataFrame(data_for_df)
        return len(self.df_consolidated), len(sorted_dates)
    
    def apply_corporate_actions(self):
        """Apply corporate actions to blank out cells before split dates"""
        if self.df_consolidated is None:
            return
        
        # Handle stock splits
        for split in self.corporate_actions.get('splits', []):
            old_symbol = split.get('old_symbol', '').strip()
            new_symbols = split.get('new_symbols', [])
            split_date = split.get('split_date', '')
            
            if old_symbol in self.df_consolidated['Symbol'].values:
                # Find columns before split date
                date_columns = [col for col in self.df_consolidated.columns 
                              if col not in ['Symbol', 'Company Name']]
                
                split_datetime = self._parse_date_string(split_date)
                if split_datetime:
                    for date_col in date_columns:
                        col_datetime = self._parse_date_string(date_col)
                        if col_datetime and col_datetime < split_datetime:
                            self.df_consolidated.loc[
                                self.df_consolidated['Symbol'] == old_symbol, 
                                date_col
                            ] = None
        
        # Handle name changes
        for change in self.corporate_actions.get('name_changes', []):
            old_symbol = change.get('old_symbol', '').strip()
            new_symbol = change.get('new_symbol', '').strip()
            change_date = change.get('change_date', '')
            
            if old_symbol in self.df_consolidated['Symbol'].values:
                date_columns = [col for col in self.df_consolidated.columns 
                              if col not in ['Symbol', 'Company Name']]
                
                change_datetime = self._parse_date_string(change_date)
                if change_datetime:
                    for date_col in date_columns:
                        col_datetime = self._parse_date_string(date_col)
                        if col_datetime and col_datetime < change_datetime:
                            self.df_consolidated.loc[
                                self.df_consolidated['Symbol'] == old_symbol,
                                date_col
                            ] = None
    
    def format_excel_output(self, output_path):
        """Format and save to Excel with styling"""
        wb = Workbook()
        ws = wb.active
        ws.title = "Market Cap Data"
        
        # Header styling
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF", size=11)
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Write headers
        headers = self.df_consolidated.columns.tolist()
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.value = header
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment
            cell.border = border
        
        # Write data
        number_format = '#,##0.00'
        data_alignment = Alignment(horizontal="right", vertical="center")
        
        for row_num, row in enumerate(self.df_consolidated.values, 2):
            for col_num, value in enumerate(row, 1):
                cell = ws.cell(row=row_num, column=col_num)
                
                if col_num <= 2:  # Symbol and Company Name columns
                    cell.value = value
                    cell.alignment = Alignment(horizontal="left", vertical="center")
                else:  # Market cap values
                    if pd.notna(value):
                        cell.value = value
                        cell.number_format = number_format
                        cell.alignment = data_alignment
                    else:
                        cell.value = None
                
                cell.border = border
        
        # Adjust column widths
        ws.column_dimensions['A'].width = 15  # Symbol
        ws.column_dimensions['B'].width = 30  # Company Name
        
        for col_num in range(3, len(headers) + 1):
            ws.column_dimensions[get_column_letter(col_num)].width = 18
        
        # Freeze panes
        ws.freeze_panes = "C2"
        
        wb.save(output_path)
    
    def consolidate(self, output_path):
        """Run complete consolidation"""
        self.load_and_consolidate_data()
        self.apply_corporate_actions()
        self.format_excel_output(output_path)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'message': 'Market Cap Consolidation Service is running'
    }), 200

@app.route('/api/consolidate', methods=['POST'])
def consolidate():
    """
    Main endpoint for consolidating market cap data
    
    Expected form data:
    - files: Multiple CSV files
    - corporate_actions: JSON string with corporate actions config (optional)
    """
    try:
        # Check if files were uploaded
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        
        if not files or len(files) == 0:
            return jsonify({'error': 'No files selected'}), 400
        
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
            
            # Parse corporate actions if provided
            corporate_actions = {
                "splits": [],
                "name_changes": [],
                "delistings": []
            }
            
            if 'corporate_actions' in request.form:
                try:
                    corporate_actions = json.loads(request.form.get('corporate_actions'))
                except json.JSONDecodeError:
                    pass
            
            # Consolidate data
            consolidator = MarketCapConsolidator(request_folder, corporate_actions)
            companies_count, dates_count = consolidator.load_and_consolidate_data()
            consolidator.apply_corporate_actions()
            
            # Create output file
            output_path = os.path.join(request_folder, 'Finished_Product.xlsx')
            consolidator.format_excel_output(output_path)
            
            # Send file
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
            
            # Parse corporate actions
            corporate_actions = {
                "splits": [],
                "name_changes": [],
                "delistings": []
            }
            
            if 'corporate_actions' in request.form:
                try:
                    corporate_actions = json.loads(request.form.get('corporate_actions'))
                except json.JSONDecodeError:
                    pass
            
            # Consolidate data for preview
            consolidator = MarketCapConsolidator(request_folder, corporate_actions)
            companies_count, dates_count = consolidator.load_and_consolidate_data()
            consolidator.apply_corporate_actions()
            
            # Get preview data (first 10 rows, first 5 columns)
            preview_df = consolidator.df_consolidated.iloc[:10]
            
            return jsonify({
                'success': True,
                'summary': {
                    'total_companies': len(consolidator.df_consolidated),
                    'total_dates': dates_count,
                    'uploaded_files': file_count,
                    'dates': [d[0] for d in consolidator.dates_list]
                },
                'preview': {
                    'columns': consolidator.df_consolidated.columns.tolist(),
                    'data': preview_df.values.tolist()
                }
            }), 200
        
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
        "save_to_file": true/false  # If true, saves mcapDDMMYYYY.csv to Backend/nosubject/
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
                
                # Priority: Look for market cap file first, then any CSV with bhav/bh in name
                csv_file = None
                
                # Priority 1: Look for mcap file (e.g., mcap03122025.csv)
                for file in file_list:
                    if file.lower().startswith('mcap') and file.lower().endswith('.csv'):
                        csv_file = file
                        print(f"Found market cap file: {csv_file}")
                        break
                
                # Priority 2: Look for Bhavcopy file (e.g., bh03122025.csv or pr03122025.csv)
                if not csv_file:
                    for file in file_list:
                        if file.lower().endswith('.csv') and ('bhav' in file.lower() or file.lower().startswith('bh') or file.lower().startswith('pr')):
                            csv_file = file
                            print(f"Found Bhavcopy file: {csv_file}")
                            break
                
                # Priority 3: Any CSV file
                if not csv_file:
                    for file in file_list:
                        if file.lower().endswith('.csv'):
                            csv_file = file
                            print(f"Found CSV file: {csv_file}")
                            break
                
                if not csv_file:
                    return jsonify({'error': f'No CSV file found in ZIP. Files available: {", ".join(file_list)}'}), 400
                
                # Extract CSV content
                csv_content = zip_ref.read(csv_file)
                csv_text = csv_content.decode('utf-8')
                
                # Parse CSV to DataFrame
                df = pd.read_csv(BytesIO(csv_content))
                
                print(f"CSV loaded. Columns: {df.columns.tolist()}")
                print(f"Total records: {len(df)}")
                
                # NSE mcap file format: Trade Date, Symbol, Series, Security Name, Category, Last Trade Date, Face Value(Rs.), Issue Size, Close Price/Paid up value(Rs.), Market Cap(Rs.)
                
                # Check if we can extract the data we need
                if 'Symbol' not in df.columns:
                    return jsonify({'error': f'Symbol column not found in CSV. Available columns: {df.columns.tolist()}'}), 400
                
                if 'Market Cap(Rs.)              ' not in df.columns and 'Market Cap(Rs.)' not in df.columns:
                    return jsonify({'error': f'Market Cap column not found in CSV. Available columns: {df.columns.tolist()}'}), 400
                
                # Create output filename
                output_filename = f"mcap{filename_date}.csv"
                output_path = os.path.join(
                    os.path.dirname(__file__),
                    'nosubject',
                    output_filename
                )
                
                # Ensure nosubject directory exists
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                if save_to_file:
                    # Save the original CSV with proper naming
                    df.to_csv(output_path, index=False)
                    print(f"Saved to: {output_path}")
                    
                    return jsonify({
                        'success': True,
                        'message': f'File downloaded and saved as {output_filename}',
                        'file': output_filename,
                        'path': output_path,
                        'date': nse_date_formatted,
                        'records_count': len(df),
                        'columns': df.columns.tolist()
                    }), 200
                else:
                    # Return CSV content as attachment
                    csv_buffer = BytesIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_buffer.seek(0)
                    
                    return send_file(
                        csv_buffer,
                        mimetype='text/csv',
                        as_attachment=True,
                        download_name=output_filename
                    )
        
        except zipfile.BadZipFile:
            return jsonify({'error': 'Invalid ZIP file received from NSE'}), 400
        except Exception as e:
            return jsonify({'error': f'Error extracting CSV: {str(e)}'}), 500
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/nse-dates', methods=['GET'])
def get_nse_dates():
    """
    Get available dates for NSE data (last 30 days)
    Returns list of dates in DD-Mon-YYYY format
    """
    try:
        dates = []
        today = datetime.now()
        
        # Generate dates for last 30 days (NSE trading days)
        for i in range(30):
            date = today - timedelta(days=i)
            # Skip weekends (5=Saturday, 6=Sunday)
            if date.weekday() < 5:
                dates.append(date.strftime('%d-%b-%Y'))
        
        return jsonify({
            'success': True,
            'dates': dates,
            'today': today.strftime('%d-%b-%Y')
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
        "save_to_file": true          # If true, saves all CSVs
    }
    Returns summary of downloads
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
                        
                        # Save to file
                        if save_to_file:
                            output_filename = f"mcap{filename_date}.csv"
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
        
        return jsonify({
            'success': True,
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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
