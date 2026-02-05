"""
Memory-optimized export utilities for reducing ZIP Excel export size and memory usage
"""

import pandas as pd
import numpy as np
import os
import tempfile
import zipfile
import gc
from openpyxl import Workbook
from io import BytesIO
import xlsxwriter

class MemoryOptimizedExporter:
    def __init__(self, compression_level=6):
        self.compression_level = compression_level  # ZIP compression level (0-9)
        
    def optimize_dataframe(self, df):
        """
        Optimize DataFrame memory usage by converting data types
        """
        # Convert object columns to categories for repeated strings
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_values = df[col].nunique()
                total_values = len(df[col])
                # Convert to category if it will save memory (< 50% unique values)
                if unique_values / total_values < 0.5:
                    df[col] = df[col].astype('category')
        
        # Optimize numeric columns
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                # Try to downcast to smaller types
                if df[col].dtype == 'int64':
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                elif df[col].dtype == 'float64':
                    df[col] = pd.to_numeric(df[col], downcast='float')
        
        return df
    
    def create_compressed_excel_xlsxwriter(self, df, output_path, sheet_name='Data'):
        """
        Create Excel file using xlsxwriter for better compression and memory efficiency
        """
        # Optimize DataFrame first
        df_optimized = self.optimize_dataframe(df.copy())
        
        # Use xlsxwriter with compression options
        workbook = xlsxwriter.Workbook(output_path, {
            'constant_memory': True,  # Optimize for memory usage
            'tmpdir': tempfile.gettempdir(),
            'default_date_format': 'yyyy-mm-dd'
        })
        
        worksheet = workbook.add_worksheet(sheet_name)
        
        # Write headers
        for col_idx, column in enumerate(df_optimized.columns):
            worksheet.write(0, col_idx, column)
        
        # Write data in chunks to avoid memory issues
        chunk_size = 1000
        for start_row in range(0, len(df_optimized), chunk_size):
            end_row = min(start_row + chunk_size, len(df_optimized))
            chunk = df_optimized.iloc[start_row:end_row]
            
            for row_idx, (_, row) in enumerate(chunk.iterrows(), start=start_row + 1):
                for col_idx, value in enumerate(row):
                    # Handle different data types
                    if pd.isna(value):
                        worksheet.write(row_idx, col_idx, '')
                    elif isinstance(value, (int, float, np.integer, np.floating)):
                        worksheet.write(row_idx, col_idx, float(value) if not pd.isna(value) else '')
                    else:
                        worksheet.write(row_idx, col_idx, str(value))
            
            # Force garbage collection after each chunk
            gc.collect()
        
        workbook.close()
        del df_optimized
        gc.collect()

    def create_multi_sheet_excel(self, data_sheets, output_path):
        """
        Create a single Excel file with multiple sheets
        data_sheets: dict where keys are sheet names and values are DataFrames
        """
        # Use xlsxwriter with compression options
        workbook = xlsxwriter.Workbook(output_path, {
            'constant_memory': True,
            'tmpdir': tempfile.gettempdir(),
            'default_date_format': 'yyyy-mm-dd'
        })
        
        # Add header formatting
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })
        
        for sheet_name, df in data_sheets.items():
            if df is None or df.empty:
                continue
                
            # Optimize DataFrame
            df_optimized = self.optimize_dataframe(df.copy())
            
            # Create worksheet
            worksheet = workbook.add_worksheet(sheet_name)
            
            # Write headers with formatting
            for col_idx, column in enumerate(df_optimized.columns):
                worksheet.write(0, col_idx, column, header_format)
                # Auto-adjust column width
                worksheet.set_column(col_idx, col_idx, min(len(str(column)) + 2, 20))
            
            # Write data in chunks
            chunk_size = 1000
            for start_row in range(0, len(df_optimized), chunk_size):
                end_row = min(start_row + chunk_size, len(df_optimized))
                chunk = df_optimized.iloc[start_row:end_row]
                
                for row_idx, (_, row) in enumerate(chunk.iterrows(), start=start_row + 1):
                    for col_idx, value in enumerate(row):
                        if pd.isna(value):
                            worksheet.write(row_idx, col_idx, '')
                        elif isinstance(value, (int, float, np.integer, np.floating)):
                            worksheet.write(row_idx, col_idx, float(value) if not pd.isna(value) else '')
                        else:
                            worksheet.write(row_idx, col_idx, str(value))
                
                # Force garbage collection after each chunk
                gc.collect()
            
            # Add auto-filter to the sheet
            if len(df_optimized) > 0:
                worksheet.autofilter(0, 0, len(df_optimized), len(df_optimized.columns) - 1)
            
            del df_optimized
            gc.collect()
        
        workbook.close()
        gc.collect()
    
    def create_compressed_zip(self, file_paths, zip_path, max_files_per_zip=None):
        """
        Create compressed ZIP file with memory optimization
        """
        if max_files_per_zip and len(file_paths) > max_files_per_zip:
            # Split into multiple ZIP files if too many files
            return self._create_multiple_zips(file_paths, zip_path, max_files_per_zip)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=self.compression_level) as zipf:
            for file_path, arcname in file_paths.items():
                zipf.write(file_path, arcname)
                # Remove temporary file immediately after adding to ZIP
                if file_path.startswith(tempfile.gettempdir()):
                    try:
                        os.remove(file_path)
                    except:
                        pass
        
        return [zip_path]
    
    def _create_multiple_zips(self, file_paths, base_zip_path, max_files_per_zip):
        """
        Create multiple ZIP files when file count exceeds limit
        """
        zip_files = []
        file_items = list(file_paths.items())
        
        for i in range(0, len(file_items), max_files_per_zip):
            chunk = file_items[i:i + max_files_per_zip]
            
            # Create zip name with part number
            base_name = os.path.splitext(base_zip_path)[0]
            part_num = (i // max_files_per_zip) + 1
            zip_path = f"{base_name}_part{part_num}.zip"
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=self.compression_level) as zipf:
                for file_path, arcname in chunk:
                    zipf.write(file_path, arcname)
                    # Remove temporary file
                    if file_path.startswith(tempfile.gettempdir()):
                        try:
                            os.remove(file_path)
                        except:
                            pass
            
            zip_files.append(zip_path)
        
        return zip_files

class ChunkedDataProcessor:
    """
    Process large datasets in chunks to reduce memory usage
    """
    
    def __init__(self, chunk_size=1000):
        self.chunk_size = chunk_size
    
    def process_csv_files_chunked(self, file_paths, process_func):
        """
        Process multiple CSV files in chunks
        """
        results = []
        
        for file_path in file_paths:
            try:
                # Process file in chunks
                chunk_results = []
                for chunk in pd.read_csv(file_path, chunksize=self.chunk_size):
                    processed_chunk = process_func(chunk)
                    chunk_results.append(processed_chunk)
                    
                # Combine chunk results
                if chunk_results:
                    combined = pd.concat(chunk_results, ignore_index=True)
                    results.append(combined)
                    
                # Clear memory
                del chunk_results
                gc.collect()
                
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue
        
        return results
    
    def consolidate_chunked(self, dataframes):
        """
        Consolidate multiple DataFrames efficiently
        """
        if not dataframes:
            return pd.DataFrame()
        
        # Process in chunks if too many DataFrames
        if len(dataframes) > 10:
            consolidated_chunks = []
            
            for i in range(0, len(dataframes), 10):
                chunk = dataframes[i:i + 10]
                combined_chunk = pd.concat(chunk, ignore_index=True)
                consolidated_chunks.append(combined_chunk)
                
                # Clear memory
                del chunk
                gc.collect()
            
            # Final consolidation
            result = pd.concat(consolidated_chunks, ignore_index=True)
            del consolidated_chunks
            gc.collect()
            
            return result
        else:
            return pd.concat(dataframes, ignore_index=True)

def reduce_excel_file_size(input_path, output_path):
    """
    Reduce Excel file size by removing unnecessary formatting and optimizing data
    """
    try:
        # Read with minimal formatting
        df = pd.read_excel(input_path, engine='openpyxl')
        
        # Optimize DataFrame
        optimizer = MemoryOptimizedExporter()
        df_optimized = optimizer.optimize_dataframe(df)
        
        # Save with xlsxwriter for better compression
        optimizer.create_compressed_excel_xlsxwriter(df_optimized, output_path)
        
        return True
    except Exception as e:
        print(f"Error reducing file size: {e}")
        return False

def get_memory_usage_mb():
    """
    Get current memory usage in MB
    """
    import psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024