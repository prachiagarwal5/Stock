import time
import requests
import pandas as pd


class SymbolMetricsFetcher:
    BASE_URL = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
    HOME_URL = "https://www.nseindia.com"

    def __init__(self, user_agent=None, timeout=10):
        self.session = requests.Session()
        self.timeout = timeout
        self.headers = {
            'User-Agent': user_agent or 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json,text/plain,*/*',
            'Connection': 'keep-alive'
        }
        self._prime_cookies()

    def _prime_cookies(self):
        try:
            self.session.get(self.HOME_URL, headers=self.headers, timeout=10)
        except Exception as exc:  # pragma: no cover - best effort warmup
            print(f"⚠️ NSE cookie warmup failed: {exc}")

    def _to_float(self, value):
        try:
            if value in [None, '', '-', 'NA']:
                return None
            return float(value)
        except Exception:
            return None

    def _call_symbol(self, symbol, series):
        params = {
            'functionName': 'getSymbolData',
            'marketType': 'N',
            'series': series,
            'symbol': symbol
        }
        resp = self.session.get(self.BASE_URL, params=params, headers=self.headers, timeout=self.timeout)
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

    def fetch_symbol_data(self, symbol, series='EQ'):
        fallback_series = [series] + [s for s in ['BE', 'BZ', 'SM', 'ST', 'E1', 'E2'] if s != series]
        last_exc = None
        data = None
        used_series = series
        for ser in fallback_series:
            try:
                data = self._call_symbol(symbol, ser)
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
            'applicableMargin': data.get('applicableMargin') or trade_info.get('applicableMargin')
        }
        return result

    def fetch_many(self, symbols, sleep_between=0.05, max_symbols=None):
        rows = []
        errors = []
        capped_symbols = symbols[:max_symbols] if max_symbols else symbols
        for sym in capped_symbols:
            try:
                rows.append(self.fetch_symbol_data(sym))
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

    def build_dashboard(self, symbols, excel_path=None, max_symbols=None):
        rows, errors = self.fetch_many(symbols, max_symbols=max_symbols)

        df = pd.DataFrame(rows)
        numeric_fields = ['impact_cost', 'free_float_mcap', 'total_market_cap', 'total_traded_value', 'last_price']
        averages = {}
        for field in numeric_fields:
            series = df[field].dropna() if field in df else pd.Series(dtype=float)
            averages[field] = round(series.mean(), 2) if not series.empty else None

        if excel_path:
            df_copy = df.copy()
            if 'indexList' in df_copy.columns:
                df_copy['indexList'] = df_copy['indexList'].apply(lambda x: ', '.join(x) if isinstance(x, list) else (x or ''))
            else:
                df_copy['indexList'] = ''

            if 'index' not in df_copy.columns:
                df_copy['index'] = ''

            avg_row = {'symbol': 'AVERAGE'}
            avg_row.update({k: averages.get(k) for k in numeric_fields})
            df_copy = pd.concat([df_copy, pd.DataFrame([avg_row])], ignore_index=True)
            df_copy.to_excel(excel_path, index=False)

        return {
            'rows': rows,
            'errors': errors,
            'averages': averages,
            'count': len(rows)
        }
