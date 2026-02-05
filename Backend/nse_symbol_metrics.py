import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
import xlsxwriter
import io


# Indices that qualify for NIFTY 500 broader index
NIFTY_500_INDICES = {
    'NIFTY 50', 'NIFTY NEXT 50', 'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 250',
    'NIFTY50', 'NIFTYNEXT50', 'NIFTYMIDCAP150', 'NIFTYSMALLCAP250'
}

# Nifty Index Constituent CSV URLs
NIFTY_INDEX_URLS = {
    'NIFTY 50': 'https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv',
    'NIFTY NEXT 50': 'https://www.niftyindices.com/IndexConstituent/ind_niftynext50list.csv',
    'NIFTY MIDCAP 150': 'https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv',
    'NIFTY SMALLCAP 250': 'https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv',
    'NIFTY MICROCAP 250': 'https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv'
}


class SymbolMetricsFetcher:
    BASE_URL = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
    HOME_URL = "https://www.nseindia.com"

    def __init__(self, user_agent=None, timeout=10):
        self.session = self._make_session(user_agent)
        self.timeout = timeout
        self.headers = {
            'User-Agent': user_agent or 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json,text/plain,*/*',
            'Connection': 'keep-alive'
        }
        self._prime_cookies(self.session)
        self.index_mapping = {}  # Cache for symbol -> indices mapping

    def fetch_nifty_indices(self, max_retries=3):
        """
        Fetch all Nifty index constituent CSV files and build a mapping
        of symbol -> list of index names.
        Includes retry logic for failed requests.
        
        Args:
            max_retries: Maximum number of retry attempts for each index (default: 3)
            
        Returns: dict {symbol: [index_name1, index_name2, ...]}
        """
        symbol_to_indices = {}
        
        # Headers to mimic browser request
        csv_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        for index_name, csv_url in NIFTY_INDEX_URLS.items():
            success = False
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        print(f"[fetch_nifty_indices] Retry {attempt}/{max_retries-1} for {index_name}...")
                    else:
                        print(f"[fetch_nifty_indices] Fetching {index_name} constituents...")
                    
                    response = requests.get(csv_url, headers=csv_headers, timeout=30)
                    response.raise_for_status()
                    
                    # Parse CSV content
                    csv_content = io.StringIO(response.text)
                    df = pd.read_csv(csv_content)
                    
                    # The CSV typically has columns like 'Symbol', 'Company Name', 'Industry', etc.
                    # We need the 'Symbol' column
                    if 'Symbol' in df.columns:
                        symbols = df['Symbol'].dropna().unique()
                        for symbol in symbols:
                            symbol = str(symbol).strip().upper()
                            if symbol not in symbol_to_indices:
                                symbol_to_indices[symbol] = []
                            symbol_to_indices[symbol].append(index_name)
                        print(f"[fetch_nifty_indices] ✓ {index_name}: {len(symbols)} symbols")
                        success = True
                        break
                    else:
                        print(f"[fetch_nifty_indices] ⚠️ {index_name}: 'Symbol' column not found in CSV")
                        last_error = "'Symbol' column not found"
                        break
                        
                except Exception as e:
                    last_error = str(e)
                    if attempt < max_retries - 1:
                        time.sleep(1)  # Brief pause before retry
                    continue
            
            if not success and last_error:
                print(f"[fetch_nifty_indices] ✗ Failed to fetch {index_name} after {max_retries} attempts: {last_error}")
        
        print(f"[fetch_nifty_indices] ✓ Total unique symbols mapped: {len(symbol_to_indices)}")
        return symbol_to_indices

    def _make_session(self, user_agent=None):
        sess = requests.Session()
        if user_agent:
            sess.headers.update({'User-Agent': user_agent})
        return sess

    def _prime_cookies(self, session=None):
        sess = session or self.session
        try:
            sess.get(self.HOME_URL, headers=self.headers, timeout=10)
        except Exception as exc:  # pragma: no cover - best effort warmup
            print(f"⚠️ NSE cookie warmup failed: {exc}")

    def _to_float(self, value):
        try:
            if value in [None, '', '-', 'NA']:
                return None
            return float(value)
        except Exception:
            return None

    def _call_symbol(self, symbol, series, session=None):
        sess = session or self.session
        params = {
            'functionName': 'getSymbolData',
            'marketType': 'N',
            'series': series,
            'symbol': symbol
        }
        
        resp = sess.get(self.BASE_URL, params=params, headers=self.headers, timeout=self.timeout)
        if resp.status_code != 200:
            raise ValueError(f"NSE getSymbolData error {resp.status_code} for {symbol} ({series})")
        
        try:
            payload = resp.json() if resp.content else {}
        except Exception:
            raise ValueError(f"Invalid JSON for {symbol} ({series})")
            
        equity_list = payload.get('equityResponse') or []
        if not equity_list:
            message = payload.get('msg') or payload.get('message') or 'No equityResponse'
            raise ValueError(f"{message} for {symbol} ({series})")
        
        return equity_list[0] or {}

    def fetch_symbol_data(self, symbol, series='EQ', as_of=None, session=None):
        as_on = as_of or datetime.now().strftime('%Y-%m-%d')
        if isinstance(as_on, datetime):
            as_on = as_on.strftime('%Y-%m-%d')

        fallback_series = [series] + [s for s in ['BE', 'BZ', 'SM', 'ST', 'E1', 'E2'] if s != series]
        last_exc = None
        data = None
        used_series = series
        for ser in fallback_series:
            try:
                data = self._call_symbol(symbol, ser, session=session)
                used_series = ser
                break
            except ValueError as exc:
                last_exc = exc
                # try next series on 404 or no equityResponse
                if 'error 404' not in str(exc) and 'No equityResponse' not in str(exc):
                    # non-retriable
                    raise
                continue

        if data is None:
            raise last_exc or ValueError(f"No data for {symbol}")

        meta = data.get('metaData', {}) or {}
        trade_info = data.get('tradeInfo', {}) or {}
        price_info = data.get('priceInfo', {}) or {}
        sec_info = data.get('secInfo', {}) or {}

        index_candidates = []
        meta_index = meta.get('index') or meta.get('indexName') or meta.get('indexNames')
        sec_index = sec_info.get('index') if isinstance(sec_info, dict) else None
        sec_indices = sec_info.get('indices') if isinstance(sec_info, dict) else None

        for candidate in [data.get('index'), meta_index, sec_index]:
            if candidate:
                if isinstance(candidate, list):
                    index_candidates.extend(candidate)
                else:
                    index_candidates.append(candidate)

        for lst in [data.get('indexList'), meta.get('indexList'), sec_indices, data.get('indices')]:
            if lst:
                if isinstance(lst, list):
                    index_candidates.extend(lst)
                else:
                    index_candidates.append(lst)

        index_clean = [idx for idx in index_candidates if idx]
        index_value = index_clean[0] if index_clean else None
        index_list = list(dict.fromkeys(index_clean))  # de-dup while preserving order

        result = {
            'symbol': data.get('symbol') or meta.get('symbol') or symbol,
            'companyName': data.get('companyName') or meta.get('companyName'),
            'series': data.get('series') or used_series,
            'status': data.get('symbolStatus') or sec_info.get('secStatus'),
            'index': index_value,
            'indexList': index_list,
            'impact_cost': self._to_float(data.get('impactCost') or trade_info.get('impactCost')),
            'free_float_mcap': self._to_float(data.get('ffmc') or trade_info.get('ffmc')),
            'total_market_cap': self._to_float(data.get('totalMarketCap') or trade_info.get('totalMarketCap')),
            'total_traded_value': self._to_float(trade_info.get('totalTradedValue') or price_info.get('totalTurnover')),
            'last_price': self._to_float(data.get('lastPrice') or trade_info.get('lastPrice')),
            'listingDate': sec_info.get('listingDate'),
            'basicIndustry': sec_info.get('basicIndustry') or data.get('industryInfo'),
            'applicableMargin': data.get('applicableMargin') or trade_info.get('applicableMargin'),
            'as_on': as_on
        }
        return result

    def fetch_many(self, symbols, sleep_between=0.02, max_symbols=None, as_of=None, parallel=True, max_workers=10, chunk_size=100, max_time_seconds=None):
        """
        Fetch symbol data with optional timeout protection.
        max_time_seconds: If set, stop fetching after this many seconds and return partial results.
        Each batch uses minimum 5 workers for optimal parallel processing.
        """
        rows = []
        errors = []
        capped_symbols = symbols[:max_symbols] if max_symbols else symbols
        start_time = time.time()

        if parallel and max_workers and max_workers > 1:
            def _worker(sym):
                # Check time budget before making request
                if max_time_seconds and (time.time() - start_time) > max_time_seconds:
                    return ('timeout', {'symbol': sym, 'error': 'Skipped - time limit reached'})
                # Fresh session per thread to avoid session locking
                sess = self._make_session()
                self._prime_cookies(sess)
                try:
                    return ('ok', self.fetch_symbol_data(sym, as_of=as_of, session=sess))
                except Exception as exc:
                    return ('err', {'symbol': sym, 'error': str(exc)})

            for i in range(0, len(capped_symbols), chunk_size or len(capped_symbols)):
                # Check if we're running out of time
                if max_time_seconds and (time.time() - start_time) > max_time_seconds:
                    remaining = len(capped_symbols) - i
                    errors.append({'symbol': 'timeout', 'error': f'Time limit reached. {remaining} symbols skipped.'})
                    break
                    
                batch = capped_symbols[i:i + (chunk_size or len(capped_symbols))]
                # Use minimum 5 workers per batch for optimal parallel processing
                workers_for_batch = max(5, min(max_workers, len(batch)))
                batch_num = (i // (chunk_size or len(capped_symbols))) + 1
                total_batches = (len(capped_symbols) + (chunk_size or len(capped_symbols)) - 1) // (chunk_size or len(capped_symbols))
                print(f"[fetch-symbols] Sub-batch {batch_num}/{total_batches}: {len(batch)} symbols with {workers_for_batch} parallel workers")
                with ThreadPoolExecutor(max_workers=workers_for_batch) as executor:
                    for status, payload in executor.map(_worker, batch):
                        if status == 'ok':
                            rows.append(payload)
                        elif status == 'timeout':
                            # Don't add individual timeout errors, we'll add summary
                            pass
                        else:
                            errors.append(payload)
        else:
            for sym in capped_symbols:
                try:
                    rows.append(self.fetch_symbol_data(sym, as_of=as_of))
                except Exception as exc:
                    errors.append({'symbol': sym, 'error': str(exc)})
                if sleep_between:
                    time.sleep(sleep_between)

        if max_symbols and len(symbols) > max_symbols:
            errors.append({
                'symbol': 'info',
                'error': f'Symbol list truncated to {max_symbols} of {len(symbols)} to avoid timeouts'
            })

        return rows, errors

    def build_dashboard(self, symbols, excel_path=None, max_symbols=None, as_of=None, parallel=True, max_workers=50, chunk_size=100, symbol_pr_data=None, symbol_mcap_data=None, max_time_seconds=None, fetch_indices_from_csv=False, nifty_indices_collection=None):
        """
        Build dashboard with additional calculated columns.
        Optimized with minimum 5 workers per batch for parallel processing.
        
        symbol_pr_data: dict of {symbol: {'days_with_data': int, 'total_trading_days': int, 'avg_pr': float}}
        symbol_mcap_data: dict of {symbol: {'avg_mcap': float, 'avg_free_float': float, 'total_traded_value': float}}
        max_time_seconds: Optional time limit for fetching (returns partial results if exceeded)
        fetch_indices_from_csv: If True, fetch index data from Nifty indices CSV files (not recommended for production)
        nifty_indices_collection: MongoDB collection for fetching stored index data (recommended)
        """
        # Fetch index mapping from DB (preferred) or CSV files (fallback)
        if nifty_indices_collection is not None:
            print("[build_dashboard] Fetching index data from MongoDB...")
            try:
                self.index_mapping = {}
                for doc in nifty_indices_collection.find({}):
                    symbol = doc.get('symbol')
                    if symbol:
                        symbol = str(symbol).strip().upper()
                    indices = doc.get('indices', [])
                    if symbol and indices:
                        self.index_mapping[symbol] = indices
                print(f"[build_dashboard] ✓ Loaded {len(self.index_mapping)} symbols from DB")
            except Exception as e:
                print(f"[build_dashboard] ⚠️ Failed to load from DB: {e}")
                self.index_mapping = {}
        elif fetch_indices_from_csv:
            print("[build_dashboard] Fetching index data from Nifty indices CSV files...")
            self.index_mapping = self.fetch_nifty_indices()
        else:
            self.index_mapping = {}
        
        # Ensure minimum 5 workers for optimal parallel processing in batches
        effective_workers = max(5, max_workers) if parallel else 1
        rows, errors = self.fetch_many(symbols, max_symbols=max_symbols, as_of=as_of, parallel=parallel, max_workers=effective_workers, chunk_size=chunk_size, max_time_seconds=max_time_seconds)

        # Store API indices before overriding
        for row in rows:
            row['index_from_api'] = row.get('index')
            row['indexList_from_api'] = row.get('indexList', [])

        # Override index data with DB/CSV data if available
        if self.index_mapping:
            replaced_index_count = 0
            not_found_symbols = []
            for row in rows:
                symbol = str(row.get('symbol') or '').strip().upper()
                if symbol and symbol in self.index_mapping:
                    csv_indices = self.index_mapping[symbol]
                    # Replace index and indexList with data from DB/CSV
                    row['index'] = csv_indices[0] if csv_indices else None
                    row['indexList'] = csv_indices
                    replaced_index_count += 1
                elif symbol:
                    not_found_symbols.append(symbol)
                    # Strictly use indices from DB - clear if not found
                    if nifty_indices_collection is not None:
                        row['index'] = None
                        row['indexList'] = []
            
            if replaced_index_count > 0:
                source = "MongoDB" if nifty_indices_collection is not None else "Nifty CSV files"
                print(f"[build_dashboard] ✓ Replaced index data for {replaced_index_count}/{len(rows)} symbols from {source}")
            
            if not_found_symbols:
                if len(not_found_symbols) <= 10:
                    print(f"[build_dashboard] ⚠️ {len(not_found_symbols)} symbols not found in DB: {', '.join(not_found_symbols)}")
                else:
                    print(f"[build_dashboard] ⚠️ {len(not_found_symbols)} symbols not found in index database")
                
                # Report these as informational errors to the frontend
                for sym in not_found_symbols:
                    errors.append({
                        'symbol': sym,
                        'error': 'Symbol not found in Nifty Index database. Index data cleared.'
                    })
        
        # Override API values with data from symbol_mcap_data if available
        replaced_mcap = 0
        replaced_traded = 0
        if symbol_mcap_data:
            for row in rows:
                symbol = row.get('symbol')
                if symbol and symbol in symbol_mcap_data:
                    mcap_info = symbol_mcap_data[symbol]
                    # Replace total_market_cap with avg_mcap from Excel data
                    if 'avg_mcap' in mcap_info and mcap_info['avg_mcap'] is not None:
                        row['total_market_cap'] = mcap_info['avg_mcap']
                        replaced_mcap += 1
                    # Replace free_float_mcap with avg_free_float from Excel data
                    if 'avg_free_float' in mcap_info and mcap_info['avg_free_float'] is not None:
                        row['free_float_mcap'] = mcap_info['avg_free_float']
                    # Replace total_traded_value with value from Excel data
                    if 'total_traded_value' in mcap_info and mcap_info['total_traded_value'] is not None:
                        row['total_traded_value'] = mcap_info['total_traded_value']
                        replaced_traded += 1
            if replaced_mcap > 0 or replaced_traded > 0:
                print(f"[build_dashboard] ✓ Replaced {replaced_mcap} market cap values and {replaced_traded} traded values from DB")
            else:
                print(f"⚠️ [build_dashboard] No values replaced. symbol_mcap_data has {len(symbol_mcap_data)} entries but none matched symbols")

        df = pd.DataFrame(rows)
        numeric_fields = ['impact_cost', 'free_float_mcap', 'total_market_cap', 'total_traded_value', 'last_price']
        averages = {}
        for field in numeric_fields:
            series = df[field].dropna() if field in df else pd.Series(dtype=float)
            averages[field] = round(series.mean(), 2) if not series.empty else None

        if excel_path and not df.empty:
            df_copy = df.copy()
            
            # Calculate Broader Index - "Nifty 500" if in qualifying indices
            # Updated Broader Index logic to prioritize indices from 'Index (DB)' over 'Index (API)'
            def calc_broader_index(index_list_db, index_list_api):
                # Strictly use 'Index (DB)' per user request
                index_list = index_list_db
                if not index_list:
                    return ''
                indices = index_list if isinstance(index_list, list) else [index_list]
                for idx in indices:
                    if idx in NIFTY_500_INDICES:
                        return 'Nifty 500'
                return ''
            
            df_copy['Broader Index'] = df_copy.apply(
                lambda row: calc_broader_index(row.get('indexList'), row.get('indexList_from_api')),
                axis=1
            )
            
            # Parse listingDate to datetime for clean display and flags
            if 'listingDate' in df_copy.columns:
                df_copy['listingDate_dt'] = pd.to_datetime(df_copy['listingDate'], errors='coerce')
            else:
                df_copy['listingDate_dt'] = pd.NaT

            today = datetime.now()

            def calc_listed_flag(date_val, months):
                if pd.isna(date_val):
                    return ''
                threshold = today - relativedelta(months=months)
                return 'Y' if date_val <= threshold else 'N'

            df_copy['listed> 6months'] = df_copy['listingDate_dt'].apply(lambda x: calc_listed_flag(x, 6))
            df_copy['listed> 1 months'] = df_copy['listingDate_dt'].apply(lambda x: calc_listed_flag(x, 1))
            df_copy['listingDate'] = df_copy['listingDate_dt']
            
            # Calculate % of traded days (from PR data if available)
            # Calculate Ratio of avg free float to avg total market cap
            def calc_ff_ratio(symbol):
                # Prefer aggregated averages if present
                if symbol_mcap_data and symbol in symbol_mcap_data:
                    mcap_info = symbol_mcap_data[symbol]
                    avg_ff = mcap_info.get('avg_free_float')
                    avg_total = mcap_info.get('avg_mcap')
                    if avg_ff and avg_total and avg_total > 0:
                        return round(avg_ff / avg_total, 4)

                # Fallback to current row values from the sheet
                row = df_copy[df_copy['symbol'] == symbol]
                if row.empty:
                    return None
                ff = row['free_float_mcap'].values[0] if 'free_float_mcap' in row.columns else None
                total = row['total_market_cap'].values[0] if 'total_market_cap' in row.columns else None
                if ff and total and total > 0:
                    return round(ff / total, 4)
                return None
            
            df_copy['Ratio of avg FF to avg Total Mcap'] = df_copy['symbol'].apply(calc_ff_ratio)
            
            # Convert indexList to string for Excel
            if 'indexList' in df_copy.columns:
                df_copy['indexList'] = df_copy['indexList'].apply(lambda x: ', '.join(x) if isinstance(x, list) else (x or ''))
            else:
                df_copy['indexList'] = ''

            # Convert API indices to string for Excel
            if 'indexList_from_api' in df_copy.columns:
                df_copy['indexList_from_api'] = df_copy['indexList_from_api'].apply(lambda x: ', '.join(x) if isinstance(x, list) else (x or ''))
            else:
                df_copy['indexList_from_api'] = ''

            if 'index' not in df_copy.columns:
                df_copy['index'] = ''
            
            if 'index_from_api' not in df_copy.columns:
                df_copy['index_from_api'] = ''

            # Reorder columns for better readability
            preferred_order = [
                'symbol', 'companyName', 'series', 'status', 
                'Broader Index', 'index', 'listingDate', 'listed> 6months', 'listed> 1 months',
                'impact_cost', 'free_float_mcap', 'total_market_cap', 
                'total_traded_value', 'last_price',
                'Ratio of avg FF to avg Total Mcap',
                'basicIndustry', 'applicableMargin', 'as_on'
            ]
            existing_cols = [c for c in preferred_order if c in df_copy.columns]
            other_cols = [c for c in df_copy.columns if c not in preferred_order]
            df_copy = df_copy[existing_cols + other_cols]
            
            # Sort by index (Index (DB)) ascending, then by symbol
            if 'index' in df_copy.columns:
                # Ensure we handle None values for sorting (put them at the end)
                df_copy = df_copy.sort_values(by=['index', 'symbol'], ascending=[True, True], na_position='last')

            avg_row = {'symbol': 'AVERAGE'}
            avg_row.update({k: averages.get(k) for k in numeric_fields})
            df_copy = pd.concat([df_copy, pd.DataFrame([avg_row])], ignore_index=True)
            # Clean NaN/INF for safe Excel writing and coerce listingDate to datetime for formatting
            df_copy = df_copy.replace([np.inf, -np.inf], np.nan)
            if 'listingDate' in df_copy.columns:
                df_copy['listingDate'] = pd.to_datetime(df_copy['listingDate'], errors='coerce')
            if 'as_on' in df_copy.columns:
                df_copy['as_on'] = pd.to_datetime(df_copy['as_on'], errors='coerce')
            df_copy = df_copy.where(pd.notna(df_copy), None)

            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                sheet_name = 'Symbol Dashboard'
                df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                wb = writer.book

                header_fmt = wb.add_format({
                    'bold': True,
                    'font_color': '#FFFFFF',
                    'bg_color': '#4F81BD',
                    'border': 1,
                    'align': 'center',
                    'valign': 'vcenter',
                    'text_wrap': True
                })
                text_fmt = wb.add_format({'border': 1, 'align': 'left'})
                int_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '0'})
                num_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '#,##0.00'})
                pct_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '0.00'})
                ratio_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '0.0000'})
                date_fmt = wb.add_format({'border': 1, 'align': 'left', 'num_format': 'yyyy-mm-dd'})

                # Rewrite headers with format
                for col_idx, col_name in enumerate(df_copy.columns):
                    ws.write(0, col_idx, col_name, header_fmt)

                # Column-specific widths and formats
                col_config = {
                    'symbol': (15, text_fmt),
                    'companyName': (30, text_fmt),
                    'series': (8, text_fmt),
                    'status': (10, text_fmt),
                    'Broader Index': (15, text_fmt),
                    'index': (20, text_fmt),
                    'listingDate': (14, date_fmt),
                    'listed> 6months': (12, text_fmt),
                    'listed> 1 months': (12, text_fmt),
                    'impact_cost': (12, num_fmt),
                    'free_float_mcap': (16, num_fmt),
                    'total_market_cap': (16, num_fmt),
                    'total_traded_value': (16, num_fmt),
                    'last_price': (12, num_fmt),
                    'Ratio of avg FF to avg Total Mcap': (18, ratio_fmt),
                    'basicIndustry': (20, text_fmt),
                    'applicableMargin': (14, int_fmt),
                    'as_on': (14, date_fmt)
                }

                for idx, col_name in enumerate(df_copy.columns):
                    width, fmt = col_config.get(col_name, (14, text_fmt))
                    ws.set_column(idx, idx, width, fmt)

                ws.freeze_panes(1, 2)
                ws.autofilter(0, 0, len(df_copy), len(df_copy.columns) - 1)
                ws.set_row(0, 22)

        return {
            'rows': rows,
            'errors': errors,
            'averages': averages,
            'count': len(rows)
        }
    def _save_dashboard_excel(self, rows, excel_path, symbol_pr_data=None, symbol_mcap_data=None):
        """Save dashboard rows to Excel file (extracted for batch processing)."""
        if not rows or not excel_path:
            return
        
        # Override API values with data from symbol_mcap_data if available
        if symbol_mcap_data:
            for row in rows:
                symbol = row.get('symbol')
                if symbol and symbol in symbol_mcap_data:
                    mcap_info = symbol_mcap_data[symbol]
                    # Replace total_market_cap with avg_mcap from Excel data
                    if 'avg_mcap' in mcap_info and mcap_info['avg_mcap'] is not None:
                        row['total_market_cap'] = mcap_info['avg_mcap']
                    # Replace total_traded_value with value from Excel data
                    if 'total_traded_value' in mcap_info and mcap_info['total_traded_value'] is not None:
                        row['total_traded_value'] = mcap_info['total_traded_value']
        
        df = pd.DataFrame(rows)
        numeric_fields = ['impact_cost', 'free_float_mcap', 'total_market_cap', 'total_traded_value', 'last_price']
        averages = {}
        for field in numeric_fields:
            series = df[field].dropna() if field in df.columns else pd.Series(dtype=float)
            averages[field] = round(series.mean(), 2) if not series.empty else None
        
        df_copy = df.copy()
        
        # Calculate Broader Index - "Nifty 500" if in qualifying indices
        # Updated Broader Index logic to prioritize indices from 'Index (DB)' over 'Index (API)'
        def calc_broader_index(index_list_db, index_list_api):
            index_list = index_list_db or index_list_api  # Prefer 'Index (DB)' if available
            if not index_list:
                return ''
            indices = index_list if isinstance(index_list, list) else [index_list]
            for idx in indices:
                if idx in NIFTY_500_INDICES:
                    return 'Nifty 500'
            return ''
        
        df_copy['Broader Index'] = df_copy.apply(
            lambda row: calc_broader_index(row.get('indexList'), row.get('indexList_from_api')),
            axis=1
        ) if 'indexList' in df_copy.columns else ''
        
        # Parse listingDate to datetime for clean display and flags
        if 'listingDate' in df_copy.columns:
            df_copy['listingDate_dt'] = pd.to_datetime(df_copy['listingDate'], errors='coerce')
        else:
            df_copy['listingDate_dt'] = pd.NaT

        today = datetime.now()

        def calc_listed_flag(date_val, months):
            if pd.isna(date_val):
                return ''
            threshold = today - relativedelta(months=months)
            return 'Y' if date_val <= threshold else 'N'

        df_copy['listed> 6months'] = df_copy['listingDate_dt'].apply(lambda x: calc_listed_flag(x, 6))
        df_copy['listed> 1 months'] = df_copy['listingDate_dt'].apply(lambda x: calc_listed_flag(x, 1))
        df_copy['listingDate'] = df_copy['listingDate_dt']
        
        # Calculate Ratio of avg free float to avg total market cap
        def calc_ff_ratio(symbol):
            if symbol_mcap_data and symbol in symbol_mcap_data:
                mcap_info = symbol_mcap_data[symbol]
                avg_ff = mcap_info.get('avg_free_float')
                avg_total = mcap_info.get('avg_mcap')
                if avg_ff and avg_total and avg_total > 0:
                    return round(avg_ff / avg_total, 4)
            row = df_copy[df_copy['symbol'] == symbol]
            if row.empty:
                return None
            ff = row['free_float_mcap'].values[0] if 'free_float_mcap' in row.columns else None
            total = row['total_market_cap'].values[0] if 'total_market_cap' in row.columns else None
            if ff and total and total > 0:
                return round(ff / total, 4)
            return None
        
        df_copy['Ratio of avg FF to avg Total Mcap'] = df_copy['symbol'].apply(calc_ff_ratio)
        
        # Convert indexList to string for Excel
        if 'indexList' in df_copy.columns:
            df_copy['indexList'] = df_copy['indexList'].apply(lambda x: ', '.join(x) if isinstance(x, list) else (x or ''))
        else:
            df_copy['indexList'] = ''

        if 'index' not in df_copy.columns:
            df_copy['index'] = ''

        # Reorder columns for better readability
        preferred_order = [
            'symbol', 'companyName', 'series', 'status', 
            'Broader Index', 'index', 'listingDate', 'listed> 6months', 'listed> 1 months',
            'impact_cost', 'free_float_mcap', 'total_market_cap', 
            'total_traded_value', 'last_price',
            'Ratio of avg FF to avg Total Mcap',
            'basicIndustry', 'applicableMargin', 'as_on'
        ]
        existing_cols = [c for c in preferred_order if c in df_copy.columns]
        other_cols = [c for c in df_copy.columns if c not in preferred_order]
        df_copy = df_copy[existing_cols + other_cols]

        avg_row = {'symbol': 'AVERAGE'}
        avg_row.update({k: averages.get(k) for k in numeric_fields})
        df_copy = pd.concat([df_copy, pd.DataFrame([avg_row])], ignore_index=True)
        
        # Clean NaN/INF for safe Excel writing
        df_copy = df_copy.replace([np.inf, -np.inf], np.nan)
        if 'listingDate' in df_copy.columns:
            df_copy['listingDate'] = pd.to_datetime(df_copy['listingDate'], errors='coerce')
        if 'as_on' in df_copy.columns:
            df_copy['as_on'] = pd.to_datetime(df_copy['as_on'], errors='coerce')
        df_copy = df_copy.where(pd.notna(df_copy), None)

        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            sheet_name = 'Symbol Dashboard'
            df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            wb = writer.book

            header_fmt = wb.add_format({
                'bold': True,
                'font_color': '#FFFFFF',
                'bg_color': '#4F81BD',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter',
                'text_wrap': True
            })
            text_fmt = wb.add_format({'border': 1, 'align': 'left'})
            int_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '0'})
            num_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '#,##0.00'})
            ratio_fmt = wb.add_format({'border': 1, 'align': 'right', 'num_format': '0.0000'})
            date_fmt = wb.add_format({'border': 1, 'align': 'left', 'num_format': 'yyyy-mm-dd'})

            for col_idx, col_name in enumerate(df_copy.columns):
                ws.write(0, col_idx, col_name, header_fmt)

            col_config = {
                'symbol': (15, text_fmt),
                'companyName': (30, text_fmt),
                'series': (8, text_fmt),
                'status': (10, text_fmt),
                'Broader Index': (15, text_fmt),
                'index': (18, text_fmt),
                'listingDate': (14, date_fmt),
                'listed> 6months': (12, text_fmt),
                'listed> 1 months': (12, text_fmt),
                'impact_cost': (12, num_fmt),
                'free_float_mcap': (16, num_fmt),
                'total_market_cap': (16, num_fmt),
                'total_traded_value': (16, num_fmt),
                'last_price': (12, num_fmt),
                'Ratio of avg FF to avg Total Mcap': (18, ratio_fmt),
                'basicIndustry': (20, text_fmt),
                'applicableMargin': (14, int_fmt),
                'as_on': (14, date_fmt)
            }

            for idx, col_name in enumerate(df_copy.columns):
                width, fmt = col_config.get(col_name, (14, text_fmt))
                ws.set_column(idx, idx, width, fmt)

            ws.freeze_panes(1, 2)
            ws.autofilter(0, 0, len(df_copy), len(df_copy.columns) - 1)
            ws.set_row(0, 22)

    def execute_all_processes(self, symbols, excel_path=None, max_symbols=None, as_of=None, parallel=True, max_workers=50, chunk_size=100, symbol_pr_data=None, symbol_mcap_data=None, max_time_seconds=None, fetch_indices_from_csv=False, nifty_indices_collection=None):
        """
        Automate the entire process: fetch data, download, and build dashboard.
        This method executes all steps sequentially without user confirmation.
        """
        print("[execute_all_processes] Starting all processes sequentially...")
        try:
            # Step 1: Fetch data
            print("[execute_all_processes] Fetching data...")
            if nifty_indices_collection is not None:
                print("[execute_all_processes] ✓ Using MongoDB for index data")
            elif fetch_indices_from_csv:
                print("[execute_all_processes] ✓ Using Nifty indices CSV files for index data")
            else:
                print("[execute_all_processes] ⚠️ No index data source specified. Using empty index mapping.")
            
            self.index_mapping = nifty_indices_collection or (self.fetch_nifty_indices() if fetch_indices_from_csv else {})
            
            # Step 2: Download data
            print("[execute_all_processes] Downloading data...")
            # Here you can add code to download any additional data files if needed

            # Step 3: Build dashboard
            print("[execute_all_processes] Building dashboard...")
            result = self.build_dashboard(symbols, excel_path=excel_path, max_symbols=max_symbols, as_of=as_of, parallel=parallel, max_workers=max_workers, chunk_size=chunk_size, symbol_pr_data=symbol_pr_data, symbol_mcap_data=symbol_mcap_data, max_time_seconds=max_time_seconds, fetch_indices_from_csv=fetch_indices_from_csv, nifty_indices_collection=nifty_indices_collection)

            print("[execute_all_processes] All processes completed successfully.")
            return result
        except Exception as e:
            print(f"[execute_all_processes] Error during execution: {e}")

    # Optimizing performance by storing averages locally instead of in the database
    # Added logic to store averages in memory or local files for faster access

    def store_averages_locally(self, rows):
        """Store averages locally to avoid database overhead."""
        averages = {}
        for row in rows:
            symbol = row.get('symbol')
            if symbol:
                averages[symbol] = {
                    'avg_mcap': row.get('total_market_cap'),
                    'avg_free_float': row.get('free_float_mcap'),
                    'total_traded_value': row.get('total_traded_value')
                }
        return averages

    def build_dashboard_with_local_averages(self, rows, averages):
        """Build dashboard using locally stored averages."""
        for row in rows:
            symbol = row.get('symbol')
            if symbol in averages:
                avg_data = averages[symbol]
                row['avg_mcap'] = avg_data.get('avg_mcap')
                row['avg_free_float'] = avg_data.get('avg_free_float')
                row['total_traded_value'] = avg_data.get('total_traded_value')
        return rows

# Refactored example usage to correctly utilize the defined functions
fetcher = SymbolMetricsFetcher()
def fetch_data():
    """Placeholder function for data fetching."""
    print("Fetching data...")
    return []  # Return an empty list as a placeholder

rows = fetch_data()  # Replace with actual data fetching logic
local_averages = fetcher.store_averages_locally(rows)
dashboard_rows = fetcher.build_dashboard_with_local_averages(rows, local_averages)