import glob
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import xlsxwriter
import numpy as np

import pandas as pd


class MarketCapConsolidator:
    def __init__(self, data_folder, output_file='Finished_Product.xlsx', config_file='corporate_actions.json', file_type='mcap'):
        self.data_folder = data_folder
        self.output_file = os.path.join(data_folder, output_file)
        self.config_file = os.path.join(data_folder, config_file)
        self.df_consolidated = None
        self.dates_list = []
        self.file_type = file_type  # 'mcap' or 'pr'
        self.corporate_actions = self._load_corporate_actions()
        self._detect_columns()

    def _is_summary_symbol(self, symbol):
        if symbol is None:
            return False
        text = str(symbol).strip().upper()
        if not text:
            return False
        normalized = re.sub(r'[^A-Z0-9]', '', text)
        if normalized in {'TOTAL', 'LISTED', 'TOTALLISTED', 'LISTEDTOTAL'}:
            return True
        if text.startswith('TOTAL') or text.startswith('LISTED'):
            return True
        return False

    def _detect_columns(self):
        if self.file_type == 'pr':
            self.symbol_col = 'SECURITY'
            self.value_col = 'NET_TRDVAL'
            self.name_col = 'SECURITY'
            self.avg_col = 'Average Net Traded Value'
        else:
            self.symbol_col = 'Symbol'
            self.value_col = 'Market Cap(Rs.)'
            self.free_float_col = 'Free Float Market Cap'
            self.name_col = 'Security Name'
            self.avg_col = 'Average Market Cap'
            self.avg_ff_col = 'Average Free Float'
        self.days_col = 'Days With Data'

    def _load_corporate_actions(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                return json.load(f)
        return {
            "splits": [],
            "name_changes": [],
            "delistings": []
        }

    def _extract_date_from_filename(self, filename):
        match = re.search(r'mcap(\d{8})', filename)
        if match:
            d = match.group(1)
            return f"{d[0:2]}-{d[2:4]}-{d[4:8]}"
        match = re.search(r'pr(\d{8})', filename)
        if match:
            d = match.group(1)
            return f"{d[0:2]}-{d[2:4]}-{d[4:8]}"
        return None

    def _parse_date_string(self, date_str):
        try:
            return datetime.strptime(date_str, "%d-%m-%Y")
        except Exception:
            return None

    def _normalize_name(self, name):
        if not isinstance(name, str):
            name = str(name)
        name = name.lower().strip()
        name = re.sub(r'[^a-z0-9]+', ' ', name)
        return name.strip()

    def _build_mcap_lookup(self):
        lookup = {}
        pattern = os.path.join(self.data_folder, 'mcap*.csv')
        mcap_files = sorted(glob.glob(pattern))
        if not mcap_files:
            return lookup

        mcap_file = mcap_files[-1]
        try:
            df_mcap = pd.read_csv(mcap_file, usecols=['Security Name', 'Symbol'], dtype=str)
            df_mcap.columns = df_mcap.columns.str.strip()
            df_mcap = df_mcap.dropna(subset=['Security Name', 'Symbol'])
            df_mcap['name_key'] = df_mcap['Security Name'].apply(self._normalize_name)
            df_mcap['symbol_key'] = df_mcap['Symbol'].apply(self._normalize_name)

            for name_key, symbol, sec_name in zip(df_mcap['name_key'], df_mcap['Symbol'], df_mcap['Security Name']):
                if name_key and name_key not in lookup:
                    lookup[name_key] = {'symbol': symbol, 'name': sec_name}
            for symbol_key, symbol, sec_name in zip(df_mcap['symbol_key'], df_mcap['Symbol'], df_mcap['Security Name']):
                if symbol_key and symbol_key not in lookup:
                    lookup[symbol_key] = {'symbol': symbol, 'name': sec_name}
        except Exception as exc:
            print(f"Warning: could not read MCAP file {mcap_file}: {exc}")
        return lookup

    def load_and_consolidate_data(self):
        stage_start = time.perf_counter()
        pattern = os.path.join(self.data_folder, 'pr*.csv' if self.file_type == 'pr' else 'mcap*.csv')
        csv_files = sorted(glob.glob(pattern))
        if not csv_files:
            print(f"No {self.file_type.upper()} CSV files found in {self.data_folder}")
            return 0, 0

        print(f"Found {len(csv_files)} {self.file_type.upper()} CSV files")

        mcap_lookup = {}
        if self.file_type == 'pr':
            mcap_lookup = self._build_mcap_lookup()
            print(f"Loaded {len(mcap_lookup)} MCAP security names for PR filtering")

        frames = []
        self.dates_list = []

        # Parallel CSV loading to reduce wall time
        def process_file(csv_file):
            start = time.perf_counter()
            date_str_local = self._extract_date_from_filename(os.path.basename(csv_file))
            if not date_str_local:
                return None, None, {'file': csv_file, 'status': 'skipped', 'reason': 'no date', 'rows': 0, 'elapsed': time.perf_counter() - start}
            try:
                df_local = pd.read_csv(csv_file)
                df_local.columns = df_local.columns.str.strip()
            except Exception:
                return None, None, {'file': csv_file, 'status': 'error', 'reason': 'read failed', 'rows': 0, 'elapsed': time.perf_counter() - start}

            if self.symbol_col not in df_local.columns or self.value_col not in df_local.columns:
                return None, None, {'file': csv_file, 'status': 'skipped', 'reason': 'missing columns', 'rows': 0, 'elapsed': time.perf_counter() - start}

            if self.file_type == 'mcap':
                df_local = df_local[[self.symbol_col, self.name_col, self.value_col, self.free_float_col]].copy()
            else:
                df_local = df_local[[self.symbol_col, self.name_col, self.value_col]].copy()
            df_local['_date_str'] = date_str_local

            sym_upper = df_local[self.symbol_col].astype(str).str.upper()
            sym_norm = sym_upper.str.replace(r'[^A-Z0-9]', '', regex=True)
            summary_mask = sym_norm.isin({'TOTAL', 'LISTED', 'TOTALLISTED', 'LISTEDTOTAL'}) | sym_upper.str.startswith(('TOTAL', 'LISTED'))
            df_local = df_local[~summary_mask]
            if df_local.empty:
                return None, date_str_local, {'file': csv_file, 'status': 'empty after summary filter', 'rows': 0, 'elapsed': time.perf_counter() - start}

            if self.file_type == 'pr' and mcap_lookup:
                sym_map = {k: v.get('symbol') for k, v in mcap_lookup.items() if v.get('symbol')}
                name_map = {k: v.get('name') for k, v in mcap_lookup.items() if v.get('name')}

                df_local['norm_name'] = df_local[self.name_col].astype(str).str.lower().str.replace(r'[^a-z0-9]+', ' ', regex=True).str.strip()
                df_local['norm_symbol'] = df_local[self.symbol_col].astype(str).str.lower().str.replace(r'[^a-z0-9]+', ' ', regex=True).str.strip()

                mapped_symbol = df_local['norm_name'].map(sym_map).combine_first(df_local['norm_symbol'].map(sym_map))
                mapped_name = df_local['norm_name'].map(name_map).combine_first(df_local['norm_symbol'].map(name_map)).fillna(df_local[self.name_col])

                df_local['Symbol'] = mapped_symbol
                df_local['Company Name'] = mapped_name
                df_local = df_local[df_local['Symbol'].notna()]
            else:
                df_local['Symbol'] = df_local[self.symbol_col].astype(str).str.strip()
                df_local['Company Name'] = df_local[self.name_col].astype(str).str.strip()

            df_local['Value'] = pd.to_numeric(df_local[self.value_col], errors='coerce')
            if self.file_type == 'mcap':
                df_local['FF_Value'] = pd.to_numeric(df_local[self.free_float_col], errors='coerce')
                df_local = df_local[['Symbol', 'Company Name', 'Value', 'FF_Value', '_date_str']]
            else:
                df_local = df_local[['Symbol', 'Company Name', 'Value', '_date_str']]
            elapsed = time.perf_counter() - start
            if df_local is None or df_local.empty:
                return None, date_str_local, {'file': csv_file, 'status': 'empty after mapping', 'rows': 0, 'elapsed': elapsed}
            meta = {
                'file': csv_file,
                'status': 'ok',
                'rows': len(df_local),
                'elapsed': elapsed,
                'details': {
                    'after_summary_rows': len(df_local),
                    'date': date_str_local
                }
            }
            return df_local, date_str_local, meta

        workers = min(20, max(2, (os.cpu_count() or 4)))
        print(f"Loading CSVs with {workers} workers...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(process_file, csv_file): csv_file for csv_file in csv_files}
            for future in as_completed(future_map):
                df_local, date_str_local, meta = future.result()
                if meta:
                    status = meta.get('status', 'unknown')
                    rows = meta.get('rows', 0)
                    elapsed = meta.get('elapsed', 0)
                    fname = os.path.basename(meta.get('file', ''))
                    reason = meta.get('reason', '')
                    details = meta.get('details', {})
                    if status == 'ok':
                        extra = f" date={details.get('date','')} after_summary_rows={details.get('after_summary_rows', rows)}"
                        print(f"[worker] {fname}: {rows} rows in {elapsed:.2f}s{extra}")
                    elif reason:
                        print(f"[worker] {fname}: {status} ({reason}) in {elapsed:.2f}s")
                    else:
                        print(f"[worker] {fname}: {status} in {elapsed:.2f}s")
                if date_str_local:
                    self.dates_list.append((date_str_local, self._parse_date_string(date_str_local)))
                if df_local is not None and not df_local.empty:
                    frames.append(df_local)

        print(f"Loaded CSVs in {time.perf_counter() - stage_start:.2f}s; merging...")

        merge_start = time.perf_counter()

        self.dates_list = [item for item in self.dates_list if item[1] is not None]
        self.dates_list.sort(key=lambda x: x[1])
        sorted_dates = [d[0] for d in self.dates_list]
        if not frames:
            print("No symbols found after processing files")
            return 0, len(sorted_dates)

        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.dropna(subset=['Symbol'])
        df_all['_date_str'] = pd.Categorical(df_all['_date_str'], categories=sorted_dates, ordered=True)
        df_all = df_all.drop_duplicates(subset=['Symbol', '_date_str'], keep='last')

        pivot = df_all.pivot(index='Symbol', columns='_date_str', values='Value')
        pivot.reset_index(inplace=True)
        
        if self.file_type == 'mcap':
            pivot_ff = df_all.pivot(index='Symbol', columns='_date_str', values='FF_Value')
            pivot_ff.reset_index(inplace=True)
            # Add prefix to FF columns to avoid collision if necessary, but we only need the average
            ff_avg = pivot_ff[sorted_dates].mean(axis=1)
            pivot[self.avg_ff_col] = ff_avg

        name_lookup = df_all.dropna(subset=['Company Name']).drop_duplicates(subset=['Symbol'], keep='last').set_index('Symbol')['Company Name'].to_dict()
        pivot['Company Name'] = pivot['Symbol'].map(name_lookup).fillna(pivot['Symbol'])

        date_cols = [c for c in pivot.columns if isinstance(c, str) and re.match(r"\d{2}-\d{2}-\d{4}", c)]
        date_cols = sorted(date_cols, key=lambda d: datetime.strptime(d, '%d-%m-%Y'))

        numeric_dates = pivot[date_cols].apply(pd.to_numeric, errors='coerce') if date_cols else pd.DataFrame()
        pivot[self.days_col] = numeric_dates.count(axis=1) if not numeric_dates.empty else 0
        pivot[self.avg_col] = numeric_dates.mean(axis=1) if not numeric_dates.empty else None

        if self.file_type == 'mcap':
            columns_order = ['Symbol', 'Company Name', self.days_col, self.avg_col, self.avg_ff_col] + date_cols
        else:
            columns_order = ['Symbol', 'Company Name', self.days_col, self.avg_col] + date_cols
        self.df_consolidated = pivot[columns_order]

        self.df_consolidated = self.df_consolidated[~self.df_consolidated['Symbol'].apply(self._is_summary_symbol)]
        self.df_consolidated = self.df_consolidated.sort_values(by=self.avg_col, ascending=False, na_position='last').reset_index(drop=True)

        self.dates_list = [(d, datetime.strptime(d, '%d-%m-%Y')) for d in date_cols]
        companies_count = len(self.df_consolidated)
        dates_count = len(date_cols)
        print(f"\nConsolidated data: {companies_count} companies across {dates_count} dates (merge/pivot in {time.perf_counter() - merge_start:.2f}s)")
        return companies_count, dates_count

    def apply_corporate_actions(self):
        if self.df_consolidated is None:
            return

        print("\nApplying corporate actions...")
        start = time.perf_counter()

        for split in self.corporate_actions.get('splits', []):
            old_symbol = split.get('old_symbol', '').strip()
            split_date = split.get('split_date', '')
            if old_symbol not in self.df_consolidated['Symbol'].values:
                continue
            date_columns = [col for col in self.df_consolidated.columns if col not in ['Symbol', 'Company Name']]
            split_dt = self._parse_date_string(split_date)
            if not split_dt:
                continue
            for date_col in date_columns:
                col_dt = self._parse_date_string(date_col)
                if col_dt and col_dt < split_dt:
                    self.df_consolidated.loc[self.df_consolidated['Symbol'] == old_symbol, date_col] = None

        for change in self.corporate_actions.get('name_changes', []):
            old_symbol = change.get('old_symbol', '').strip()
            change_date = change.get('change_date', '')
            if old_symbol not in self.df_consolidated['Symbol'].values:
                continue
            date_columns = [col for col in self.df_consolidated.columns if col not in ['Symbol', 'Company Name']]
            change_dt = self._parse_date_string(change_date)
            if not change_dt:
                continue
            for date_col in date_columns:
                col_dt = self._parse_date_string(date_col)
                if col_dt and col_dt < change_dt:
                    self.df_consolidated.loc[self.df_consolidated['Symbol'] == old_symbol, date_col] = None

        print(f"Corporate actions done in {time.perf_counter() - start:.2f}s")

    def format_excel_output(self, output_file=None):
        if output_file:
            self.output_file = output_file

        print("Creating Excel files with parallel row prep (no formatting)...")
        start_excel = time.perf_counter()

        df = self.df_consolidated.copy()
        # Clean NaN/INF so xlsxwriter write_row doesn't fail
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.where(pd.notna(df), None)

        main_sheet = 'Market Cap Data' if self.file_type == 'mcap' else 'Net Traded Value'
        avg_headers = ['Symbol', 'Company Name', self.days_col, self.avg_col]
        avg_df = df[avg_headers].copy()

        def chunk_rows(frame, chunk_size=5000):
            values = frame.to_numpy()
            chunks = []
            for start in range(0, len(values), chunk_size):
                chunks.append(values[start:start + chunk_size])
            return chunks

        def flatten_chunks(chunks, max_workers):
            rows = []
            if not chunks:
                return rows
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [pool.submit(lambda arr: arr.tolist(), ch) for ch in chunks]
                for fut in as_completed(futures):
                    rows.extend(fut.result())
            return rows

        workers = min(20, max(2, (os.cpu_count() or 4)))

        main_chunks = chunk_rows(df)
        avg_chunks = chunk_rows(avg_df)

        print(f"Preparing rows in parallel with {workers} workers...")
        prep_start = time.perf_counter()
        main_rows = flatten_chunks(main_chunks, workers)
        avg_rows = flatten_chunks(avg_chunks, workers)
        print(f"Row prep done in {time.perf_counter() - prep_start:.2f}s")

        workbook = xlsxwriter.Workbook(self.output_file, {'constant_memory': True, 'nan_inf_to_errors': True})
        ws_main = workbook.add_worksheet(main_sheet)
        ws_avg = workbook.add_worksheet('Averages')

        # Basic formats
        header_fmt = workbook.add_format({'bold': True, 'font_color': '#FFFFFF', 'bg_color': '#4F81BD', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        text_fmt = workbook.add_format({'border': 1, 'align': 'left'})
        int_fmt = workbook.add_format({'border': 1, 'align': 'right', 'num_format': '0'})
        num_fmt = workbook.add_format({'border': 1, 'align': 'right', 'num_format': '#,##0.00'})

        # Headers
        for col_idx, col_name in enumerate(df.columns):
            ws_main.write(0, col_idx, col_name, header_fmt)
        for col_idx, col_name in enumerate(avg_df.columns):
            ws_avg.write(0, col_idx, col_name, header_fmt)

        # Column widths/formats
        ws_main.set_column(0, 0, 15, text_fmt)
        ws_main.set_column(1, 1, 30, text_fmt)
        ws_main.set_column(2, 2, 12, int_fmt)
        ws_main.set_column(3, 3, 18, num_fmt)
        if len(df.columns) > 4:
            ws_main.set_column(4, len(df.columns) - 1, 14, num_fmt)
        ws_main.freeze_panes(1, 2)

        ws_avg.set_column(0, 0, 15, text_fmt)
        ws_avg.set_column(1, 1, 30, text_fmt)
        ws_avg.set_column(2, 2, 12, int_fmt)
        ws_avg.set_column(3, 3, 18, num_fmt)
        ws_avg.freeze_panes(1, 2)

        # Data rows
        for row_idx, row in enumerate(main_rows, start=1):
            ws_main.write_row(row_idx, 0, row)
        for row_idx, row in enumerate(avg_rows, start=1):
            ws_avg.write_row(row_idx, 0, row)

        workbook.close()
        print(f"✓ Excel file created: {self.output_file} in {time.perf_counter() - start_excel:.2f}s")

    def create_corporate_actions_template(self):
        if not os.path.exists(self.config_file):
            template = {
                "splits": [
                    {
                        "old_symbol": "TATAMOTOR",
                        "new_symbols": ["TMPV", "TMCV"],
                        "split_date": "DD-MM-YYYY",
                        "description": "Stock split/demerger - blank old symbol before this date"
                    }
                ],
                "name_changes": [
                    {
                        "old_symbol": "OLDNAME",
                        "new_symbol": "NEWNAME",
                        "change_date": "DD-MM-YYYY",
                        "description": "Company name change - blank old symbol before this date"
                    }
                ],
                "delistings": [
                    {
                        "symbol": "DELISTED",
                        "delisting_date": "DD-MM-YYYY",
                        "description": "Company delisted - blank from this date onwards"
                    }
                ]
            }
            with open(self.config_file, 'w') as f:
                json.dump(template, f, indent=2)
            print(f"✓ Created corporate actions template: {self.config_file}")

    def run(self):
        file_type_name = "Net Traded Value" if self.file_type == 'pr' else "Market Cap"
        print("=" * 60)
        print(f"{file_type_name} Consolidation Tool")
        print("=" * 60)

        self.create_corporate_actions_template()

        overall_start = time.perf_counter()
        companies_count, dates_count = self.load_and_consolidate_data()
        if companies_count > 0:
            self.apply_corporate_actions()
            self.format_excel_output()
            print(f"\n✓ Consolidation complete! Total elapsed {time.perf_counter() - overall_start:.2f}s")
            return True
        print("✗ Consolidation failed!")
        return False


def main():
    data_folder = '/Users/vinayak/Desktop/Proj01/nosubject'
    consolidator = MarketCapConsolidator(data_folder)
    consolidator.run()


if __name__ == "__main__":
    main()
