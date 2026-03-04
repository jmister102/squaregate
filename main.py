"""
SquareGate Bloomberg Analyzer
GUI application for Bloomberg data collection, screening, and Excel output.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import tkinter.scrolledtext as scrolledtext
import threading
import queue
import socket
import re
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from bloomberg_collector import BloombergDataCollector
from filters import apply_filters, create_fail_reason
from excel_formatter import format_excel_columns

# ── App constants ──────────────────────────────────────────────────────────────
APP_NAME    = "SquareGate Bloomberg Analyzer"
APP_VERSION = "1.0.0"
BBG_HOST    = "localhost"
BBG_PORT    = 8194
BBG_POLL_MS = 5_000          # Bloomberg status re-check interval (ms)
GUI_POLL_MS = 100            # Queue poll interval (ms)

# Ticker format: at least two whitespace-separated tokens ending with a known type
_BBG_TYPES = r'Equity|Corp|Govt|Muni|Index|Comdty|Curncy|MMkt|Pfd|Mtge'
TICKER_RE   = re.compile(rf'^\S+(?:\s+\S+)*\s+({_BBG_TYPES})$', re.IGNORECASE)

# ── Palette ───────────────────────────────────────────────────────────────────
C_HEADER_BG = '#1a3a5c'
C_HEADER_FG = '#ffffff'
C_BG        = '#f0f0f0'
C_GREEN     = '#28a745'
C_RED       = '#dc3545'
C_AMBER     = '#e6a817'
C_BTN       = '#1a3a5c'
C_BTN_FG    = '#ffffff'
C_BTN_OFF   = '#888888'


# ── Logging → GUI queue ────────────────────────────────────────────────────────
class _QueueHandler(logging.Handler):
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q
        self.setFormatter(logging.Formatter('%(asctime)s [%(levelname)-7s] %(message)s',
                                            datefmt='%H:%M:%S'))

    def emit(self, record: logging.LogRecord):
        self.q.put(('log', record.levelname, self.format(record)))


# ── Bloomberg availability ─────────────────────────────────────────────────────
def bloomberg_reachable(host: str = BBG_HOST, port: int = BBG_PORT,
                        timeout: float = 1.0) -> bool:
    """Return True if Bloomberg API port is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ── Ticker CSV validation ──────────────────────────────────────────────────────
def validate_ticker_file(filepath: str) -> tuple[bool, str, list]:
    """
    Validate the selected CSV file.

    Returns
    -------
    (ok, message, tickers)
        ok      : True if file is valid
        message : Human-readable result summary
        tickers : Parsed ticker list (empty on failure)
    """
    try:
        with open(filepath, encoding='utf-8') as fh:
            raw = fh.readlines()
    except Exception as exc:
        return False, f"Cannot read file: {exc}", []

    # Strip blanks and comments
    tickers = [ln.strip() for ln in raw if ln.strip() and not ln.startswith('#')]

    if not tickers:
        return False, "File is empty — no tickers found.", []

    # Reject multi-column CSVs immediately
    if any(',' in t for t in tickers):
        return False, (
            "File appears to contain multiple columns (commas detected).\n"
            "Expected one ticker per line, e.g.:\n  AAPL US Equity\n  MSFT US Equity"
        ), []

    bad = [f"  Line {i}: {t!r}" for i, t in enumerate(tickers, 1)
           if not TICKER_RE.match(t)]

    if bad:
        sample = '\n'.join(bad[:5])
        extra  = f'\n  …and {len(bad) - 5} more' if len(bad) > 5 else ''
        return False, (
            f"Invalid format — expected 'AAPL US Equity' per line.\n"
            f"Problem lines:\n{sample}{extra}"
        ), []

    return True, f"{len(tickers)} ticker(s) loaded.", tickers


# ── Analysis worker (background thread) ───────────────────────────────────────
def run_analysis(tickers: list, output_dir: str, msg_q: queue.Queue):
    """
    Full data-collection pipeline. Runs in a daemon thread.

    Posts to msg_q:
        ('log',      level_str, formatted_text)
        ('progress', pct_float, status_text)
        ('done',     success_bool, summary_text)
    """
    logger = logging.getLogger('squaregate')

    def progress(pct: float, text: str):
        msg_q.put(('progress', pct, text))

    try:
        date_tag    = datetime.now().strftime('%Y%m%d_%H%M%S')
        yesterday   = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        sixty_ago   = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

        # ── Connect ───────────────────────────────────────────────────────────
        progress(2, 'Connecting to Bloomberg API…')
        collector = BloombergDataCollector()
        if not collector.connect():
            msg_q.put(('done', False, 'Failed to connect to Bloomberg API.'))
            return
        logger.info(f'Connected. Analysing {len(tickers)} ticker(s).')

        # ── Phase 1a: Standard fields ─────────────────────────────────────────
        progress(5, 'Fetching standard fields…')
        standard_fields = [
            'NAME', 'INDUSTRY_GROUP', 'PX_LAST',
            '3MO_CALL_IMP_VOL', '12MO_CALL_IMP_VOL', 'VOLATILITY_90D',
            'CUR_MKT_CAP', 'ST_DEBT', 'LT_DEBT',
            'MOST_RECENT_PERIOD_END_DT', 'LATEST_ANN_DT_QTRLY',
            'OFFERING_PRELIM_FILING_DT', 'LATEST_ANN_DT_ANNUAL',
        ]
        df_std = collector.get_reference_data(tickers, standard_fields)
        if df_std is None:
            collector.disconnect()
            msg_q.put(('done', False, 'Failed to fetch standard fields.'))
            return

        # ── Phase 1b: Override fields (one request each) ──────────────────────
        override_fields = [
            ('AVAT_100d',          'INTERVAL_AVG',                    {'CALC_INTERVAL': '100D', 'MARKET_DATA_OVERRIDE': 'TURNOVER', 'CRNCY': 'USD', 'END_DATE_OVERRIDE': yesterday, 'PERIODICITY_OVERRIDE': 'D'}, 1),
            ('AVAT_20d',           'INTERVAL_AVG',                    {'CALC_INTERVAL': '20D',  'MARKET_DATA_OVERRIDE': 'TURNOVER', 'CRNCY': 'USD', 'END_DATE_OVERRIDE': yesterday, 'PERIODICITY_OVERRIDE': 'D'}, 1),
            ('CASH_AND_EQUIVS',    'CASH_CASH_EQTY_STI_DETAILED',    {'FUND_PER': 'Q'},         1_000_000),
            ('BS_ST_BORROW',       'BS_ST_BORROW',                    {'FUND_PER': 'Q'},         1_000_000),
            ('BS_ST_LEASE_LIAB',   'ST_CAPITALIZED_LEASE_LIABILITIES',{'FUND_PER': 'Q'},         1_000_000),
            ('BS_LT_BORROW',       'BS_LT_BORROW',                    {'FUND_PER': 'Q'},         1_000_000),
            ('LT_LEASES',          'LT_CAPITALIZED_LEASE_LIABILITIES', {'FUND_PER': 'Q'},        1_000_000),
            ('CFO_Q',              'CF_CASH_FROM_OPER',               {'FUND_PER': 'Q'},         1_000_000),
            ('CFO_TTM',            'TRAIL_12M_CASH_FROM_OPER',        {'FUND_PER': 'Q'},         1_000_000),
            ('FCF_TTM',            'TRAIL_12M_FREE_CASH_FLOW',        {'FUND_PER': 'Q'},         1_000_000),
            ('FCF_Q',              'CF_FREE_CASH_FLOW',               {'FUND_PER': 'Q'},         1_000_000),
            ('EQY_FLOAT2',         'EQY_FLOAT',                       {},                        1_000_000),
            ('INTERVAL_HIGH_60D',  'INTERVAL_HIGH',                   {'START_DATE_OVERRIDE': sixty_ago}, 1),
        ]

        override_dfs: dict = {}
        n_ov = len(override_fields)
        for i, (col, field, ov, scale) in enumerate(override_fields):
            pct = 5 + (i + 1) / n_ov * 40    # 5 % → 45 %
            progress(pct, f'Fetching {col}…')
            try:
                df_tmp = collector.get_reference_data(tickers, [field],
                                                      overrides=ov or None)
                if df_tmp is not None:
                    if scale != 1:
                        df_tmp[field] = pd.to_numeric(df_tmp[field], errors='coerce') * scale
                    df_tmp.rename(columns={field: col}, inplace=True)
                    override_dfs[col] = df_tmp
                else:
                    logger.warning(f'No data returned for {col}')
            except Exception as exc:
                logger.error(f'Error fetching {col}: {exc}')

        # ── Merge Phase 1 results ─────────────────────────────────────────────
        df_final = df_std.copy()
        for col, df_ov in override_dfs.items():
            df_final = pd.concat([df_final, df_ov], axis=1)

        # ── Phase 2: Per-ticker dependent fields ──────────────────────────────
        progress(45, 'Fetching per-ticker dependent fields…')
        dep_data: dict = {}

        for ti, ticker in enumerate(tickers):
            pct = 45 + (ti + 1) / len(tickers) * 28   # 45 % → 73 %
            progress(pct, f'Dependent fields: {ticker}  ({ti + 1}/{len(tickers)})')
            try:
                raw_date = (df_final.loc[ticker, 'LATEST_ANN_DT_ANNUAL']
                            if 'LATEST_ANN_DT_ANNUAL' in df_final.columns else None)
                ann = pd.to_datetime(raw_date, errors='coerce')
                if pd.isna(ann):
                    logger.warning(f'{ticker}: no 10-K date — skipping dependent fields')
                    continue

                ann_str    = ann.strftime('%Y%m%d')
                pre60_str  = (ann - timedelta(days=60)).strftime('%Y%m%d')
                dep_data.setdefault(ticker, {})

                # INTERVAL_HIGH1: 60 days before 10-K through 10-K date
                df_ih1 = collector.get_reference_data(
                    [ticker], ['INTERVAL_HIGH'],
                    overrides={'START_DATE_OVERRIDE': pre60_str,
                               'END_DATE_OVERRIDE':   ann_str})
                if df_ih1 is not None:
                    dep_data[ticker]['INTERVAL_HIGH1'] = df_ih1.loc[ticker, 'INTERVAL_HIGH']

                # INTERVAL_HIGH2: from 10-K date to present
                df_ih2 = collector.get_reference_data(
                    [ticker], ['INTERVAL_HIGH'],
                    overrides={'START_DATE_OVERRIDE': ann_str})
                if df_ih2 is not None:
                    dep_data[ticker]['INTERVAL_HIGH2'] = df_ih2.loc[ticker, 'INTERVAL_HIGH']

                # EQY_FLOAT1: equity float as of 10-K date (BDH)
                df_ef1 = collector.get_historical_data(
                    [ticker], ['EQY_FLOAT'],
                    start_date=ann_str, end_date=ann_str)
                if df_ef1 is not None and 'EQY_FLOAT' in df_ef1.columns:
                    val = pd.to_numeric(df_ef1.loc[ticker, 'EQY_FLOAT'], errors='coerce')
                    dep_data[ticker]['EQY_FLOAT1'] = val * 1_000_000 if pd.notna(val) else None
                else:
                    logger.warning(f'{ticker}: no historical EQY_FLOAT on {ann_str}')

            except Exception as exc:
                logger.error(f'Dependent fields failed for {ticker}: {exc}')

        collector.disconnect()

        if dep_data:
            df_final = pd.concat([df_final, pd.DataFrame.from_dict(dep_data, orient='index')], axis=1)

        # ── Derived calculations ───────────────────────────────────────────────
        progress(74, 'Calculating derived columns…')
        df_final = df_final.reset_index()
        df_final.rename(columns={'index': 'Ticker'}, inplace=True)

        df_final['ST_DEBT'] = df_final['BS_ST_BORROW'] - df_final['BS_ST_LEASE_LIAB']
        df_final['LT_DEBT'] = df_final['BS_LT_BORROW'] - df_final['LT_LEASES']

        df_final['FloatValue_preFile']  = df_final['EQY_FLOAT1']  * df_final['INTERVAL_HIGH1']
        df_final['FloatValue_postFile'] = df_final['EQY_FLOAT2']  * df_final['INTERVAL_HIGH2']
        df_final['FloatValue_last60d']  = df_final['EQY_FLOAT2']  * df_final['INTERVAL_HIGH_60D']

        df_final['baby_shelf_filter'] = (
            ((df_final['FloatValue_preFile']  < 75_000_000) &
             (df_final['FloatValue_postFile'] < 75_000_000)) |
            df_final['FloatValue_preFile'].isna()  |
            df_final['FloatValue_postFile'].isna()
        ).astype(int)

        df_final['shelf_limit'] = np.where(
            df_final['baby_shelf_filter'].isna() | df_final['FloatValue_last60d'].isna(),
            '?',
            np.where(
                df_final['baby_shelf_filter'] == 1,
                (1 / 3) * np.maximum(df_final['FloatValue_preFile'],
                                     df_final['FloatValue_last60d']),
                'Unlimited',
            ),
        )

        net_cash = df_final['CASH_AND_EQUIVS'] - df_final['ST_DEBT']
        df_final['Burn_Q']   = np.where(net_cash > 0,
                                        3  * net_cash / -df_final['FCF_Q'],   'Net Debt')
        df_final['Burn_TTM'] = np.where(net_cash > 0,
                                        12 * net_cash / -df_final['FCF_TTM'], 'Net Debt')
        df_final['Burn_Q']   = pd.to_numeric(df_final['Burn_Q'],   errors='coerce').fillna('')
        df_final['Burn_TTM'] = pd.to_numeric(df_final['Burn_TTM'], errors='coerce').fillna('')

        df_final['AVAT_100d_Burn_Q']   = -df_final['FCF_Q']   / (63  * df_final['AVAT_100d'])
        df_final['AVAT_20d_Burn_Q']    = -df_final['FCF_Q']   / (63  * df_final['AVAT_20d'])
        df_final['AVAT_100d_Burn_TTM'] = -df_final['FCF_TTM'] / (252 * df_final['AVAT_100d'])
        df_final['AVAT_20d_Burn_TTM']  = -df_final['FCF_TTM'] / (252 * df_final['AVAT_20d'])

        # Annotation columns (user-filled in Excel)
        for col in ('FLAG', 'Assignment', 'eloc_filter', 'atm_filter', 'Notes'):
            df_final[col] = ''

        # Date columns
        df_final['10-K_Date']         = pd.to_datetime(df_final['LATEST_ANN_DT_ANNUAL'], errors='coerce')
        df_final['10-K_Date_minus60d']= df_final['10-K_Date'] - pd.Timedelta(days=60)
        for col in ('MOST_RECENT_PERIOD_END_DT', 'LATEST_ANN_DT_QTRLY',
                    'OFFERING_PRELIM_FILING_DT', 'LATEST_ANN_DT_ANNUAL'):
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce')

        # ── Filters ────────────────────────────────────────────────────────────
        progress(80, 'Applying screening filters…')
        filters = apply_filters(df_final)
        for name, result in filters.items():
            df_final[name] = result
        test_cols = list(filters.keys())
        df_final['fail_reason'] = create_fail_reason(df_final, test_cols)
        df_final['final_pass']  = df_final[test_cols].all(axis=1)
        n_pass = int(df_final['final_pass'].sum())
        logger.info(f'Filters complete: {n_pass}/{len(df_final)} passed.')

        # ── Output helpers ─────────────────────────────────────────────────────
        def out_path(stem: str) -> str:
            return os.path.join(output_dir, f'{stem}_{date_tag}.xlsx')

        def save(df: pd.DataFrame, stem: str):
            path = out_path(stem)
            df.to_excel(path, index=False, engine='openpyxl')
            format_excel_columns(path)
            logger.info(f'Saved → {os.path.basename(path)}')
            return path

        # Output 1: Filter results for all tickers
        progress(84, 'Writing filter results…')
        save(df_final[['Ticker'] + test_cols + ['fail_reason']],
             'filter_results')

        # Output 2: Passing tickers only
        progress(88, 'Writing passing tickers…')
        save(df_final[df_final['final_pass']][['Ticker'] + test_cols],
             'passing_tickers')

        # Output 3: Full data sheet (original column order)
        progress(92, 'Writing full data spreadsheet…')
        sheet_cols = [
            'NAME', 'INDUSTRY_GROUP', 'Ticker',
            'test_adv_gt_50k', 'eloc_filter', 'atm_filter',
            'baby_shelf_filter', 'shelf_limit',
            'PX_LAST', '3MO_CALL_IMP_VOL', '12MO_CALL_IMP_VOL', 'VOLATILITY_90D',
            'CUR_MKT_CAP', 'AVAT_100d', 'AVAT_20d',
            'CASH_AND_EQUIVS', 'BS_ST_BORROW', 'BS_ST_LEASE_LIAB', 'ST_DEBT',
            'BS_LT_BORROW', 'LT_LEASES', 'LT_DEBT',
            'CFO_Q', 'CFO_TTM', 'FCF_Q', 'FCF_TTM',
            'Assignment', 'FLAG', 'Notes',
            'MOST_RECENT_PERIOD_END_DT', 'LATEST_ANN_DT_QTRLY', 'OFFERING_PRELIM_FILING_DT',
            'Burn_Q', 'Burn_TTM',
            'AVAT_100d_Burn_Q', 'AVAT_20d_Burn_Q', 'AVAT_100d_Burn_TTM', 'AVAT_20d_Burn_TTM',
            'LATEST_ANN_DT_ANNUAL', '10-K_Date', '10-K_Date_minus60d',
            'EQY_FLOAT1', 'EQY_FLOAT2',
            'INTERVAL_HIGH1', 'INTERVAL_HIGH2', 'INTERVAL_HIGH_60D',
            'FloatValue_preFile', 'FloatValue_postFile', 'FloatValue_last60d',
        ]
        existing = [c for c in sheet_cols if c in df_final.columns]
        df_sheet = df_final[existing].copy()
        df_sheet.insert(0, '', '')   # leading blank column (original sheet style)
        save(df_sheet, 'spreadsheet_data')

        # Output 4: Raw Bloomberg dump (all columns)
        progress(96, 'Writing raw data export…')
        save(df_final, 'bloomberg_raw')

        progress(100, 'Complete ✓')
        summary = (
            f'Analysis complete.\n\n'
            f'  Tickers processed : {len(tickers)}\n'
            f'  Passed all filters: {n_pass}\n'
            f'  Output folder     : {output_dir}\n'
            f'  Run timestamp     : {date_tag}'
        )
        msg_q.put(('done', True, summary))

    except Exception as exc:
        logger.exception(f'Unhandled error in analysis: {exc}')
        msg_q.put(('done', False, f'Unexpected error:\n{exc}'))


# ── GUI application ────────────────────────────────────────────────────────────
class SquareGateApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_NAME)
        self.root.resizable(True, True)
        self.root.minsize(680, 600)

        self.msg_q:           queue.Queue = queue.Queue()
        self.tickers:         list        = []
        self.bloomberg_ok:    bool        = False
        self.analysis_active: bool        = False

        self._setup_logging()
        self._build_ui()
        self._schedule_bloomberg_check()
        self._poll_queue()

    # ── Logging ───────────────────────────────────────────────────────────────

    def _setup_logging(self):
        logger = logging.getLogger('squaregate')
        logger.setLevel(logging.DEBUG)
        logger.addHandler(_QueueHandler(self.msg_q))

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self):
        self.root.configure(bg=C_BG)

        # Header
        hdr = tk.Frame(self.root, bg=C_HEADER_BG, pady=10)
        hdr.pack(fill='x')
        tk.Label(hdr, text=APP_NAME, font=('Segoe UI', 15, 'bold'),
                 bg=C_HEADER_BG, fg=C_HEADER_FG).pack(side='left', padx=16)
        tk.Label(hdr, text=f'v{APP_VERSION}', font=('Segoe UI', 9),
                 bg=C_HEADER_BG, fg='#aaccee').pack(side='left')

        # Bloomberg status bar
        bbg_bar = tk.Frame(self.root, bg=C_HEADER_BG, pady=4)
        bbg_bar.pack(fill='x')
        tk.Label(bbg_bar, text='Bloomberg API:', font=('Segoe UI', 9, 'bold'),
                 bg=C_HEADER_BG, fg='#ccddee').pack(side='left', padx=(16, 6))
        self._bbg_dot = tk.Label(bbg_bar, text='●', font=('Segoe UI', 12),
                                 bg=C_HEADER_BG, fg=C_AMBER)
        self._bbg_dot.pack(side='left', padx=(0, 4))
        self._bbg_lbl = tk.Label(bbg_bar, text='Checking…', font=('Segoe UI', 9),
                                 bg=C_HEADER_BG, fg=C_AMBER)
        self._bbg_lbl.pack(side='left')

        ttk.Separator(self.root).pack(fill='x')

        body = tk.Frame(self.root, bg=C_BG, padx=14, pady=6)
        body.pack(fill='both', expand=True)

        # File selection
        file_box = tk.LabelFrame(body, text=' Ticker File (CSV) ', font=('Segoe UI', 10),
                                 bg=C_BG, padx=10, pady=8)
        file_box.pack(fill='x', pady=(4, 6))

        file_row = tk.Frame(file_box, bg=C_BG)
        file_row.pack(fill='x')
        self._file_var = tk.StringVar()
        tk.Entry(file_row, textvariable=self._file_var, state='readonly',
                 font=('Segoe UI', 9), relief='solid', bd=1
                 ).pack(side='left', fill='x', expand=True, ipady=4)
        tk.Button(file_row, text='Browse…', command=self._browse_file,
                  font=('Segoe UI', 9), bg=C_BTN, fg=C_BTN_FG,
                  relief='flat', padx=10, cursor='hand2'
                  ).pack(side='left', padx=(6, 0))

        self._file_status = tk.Label(file_box, text='No file selected.',
                                     font=('Segoe UI', 9), bg=C_BG, fg='#666666',
                                     justify='left', wraplength=580)
        self._file_status.pack(anchor='w', pady=(4, 0))

        # Output directory
        out_box = tk.LabelFrame(body, text=' Output Folder ', font=('Segoe UI', 10),
                                bg=C_BG, padx=10, pady=8)
        out_box.pack(fill='x', pady=(0, 6))

        out_row = tk.Frame(out_box, bg=C_BG)
        out_row.pack(fill='x')
        default_out = os.path.join(os.path.expanduser('~'), 'Documents', 'SquareGate')
        self._out_var = tk.StringVar(value=default_out)
        tk.Entry(out_row, textvariable=self._out_var, state='readonly',
                 font=('Segoe UI', 9), relief='solid', bd=1
                 ).pack(side='left', fill='x', expand=True, ipady=4)
        tk.Button(out_row, text='Browse…', command=self._browse_output,
                  font=('Segoe UI', 9), bg=C_BTN, fg=C_BTN_FG,
                  relief='flat', padx=10, cursor='hand2'
                  ).pack(side='left', padx=(6, 0))

        # Run button
        btn_frame = tk.Frame(body, bg=C_BG, pady=6)
        btn_frame.pack()
        self._run_btn = tk.Button(btn_frame, text='▶   Run Analysis',
                                  command=self._start_analysis,
                                  font=('Segoe UI', 12, 'bold'),
                                  bg=C_BTN, fg=C_BTN_FG,
                                  relief='flat', padx=24, pady=8,
                                  cursor='hand2', state='disabled')
        self._run_btn.pack()

        # Progress
        prog_frame = tk.Frame(body, bg=C_BG)
        prog_frame.pack(fill='x', pady=(2, 0))
        self._prog_bar = ttk.Progressbar(prog_frame, mode='determinate', maximum=100)
        self._prog_bar.pack(fill='x', pady=(0, 2))
        self._prog_lbl = tk.Label(prog_frame, text='', font=('Segoe UI', 9),
                                  bg=C_BG, fg='#444444')
        self._prog_lbl.pack(anchor='w')

        ttk.Separator(body).pack(fill='x', pady=(6, 0))

        # Log pane
        log_box = tk.LabelFrame(body, text=' Log ', font=('Segoe UI', 10),
                                bg=C_BG, padx=6, pady=4)
        log_box.pack(fill='both', expand=True, pady=(4, 0))

        self._log = scrolledtext.ScrolledText(
            log_box, font=('Consolas', 8), state='disabled',
            bg='#1e1e1e', fg='#d4d4d4', relief='flat', bd=0,
            wrap='word', height=10)
        self._log.pack(fill='both', expand=True)
        self._log.tag_configure('ERROR',   foreground='#f48771')
        self._log.tag_configure('WARNING', foreground='#cca700')
        self._log.tag_configure('INFO',    foreground='#9cdcfe')
        self._log.tag_configure('DEBUG',   foreground='#666666')

    # ── Bloomberg status polling ───────────────────────────────────────────────

    def _schedule_bloomberg_check(self):
        threading.Thread(target=self._bloomberg_worker, daemon=True).start()

    def _bloomberg_worker(self):
        ok = bloomberg_reachable()
        self.root.after(0, lambda: self._update_bloomberg_ui(ok))
        self.root.after(BBG_POLL_MS, self._schedule_bloomberg_check)

    def _update_bloomberg_ui(self, ok: bool):
        if ok == self.bloomberg_ok:
            return
        self.bloomberg_ok = ok
        if ok:
            self._bbg_dot.config(fg=C_GREEN)
            self._bbg_lbl.config(fg=C_GREEN, text='Running  (port 8194 reachable)')
        else:
            self._bbg_dot.config(fg=C_RED)
            self._bbg_lbl.config(fg=C_RED,
                                 text='Not detected  — Bloomberg Terminal must be running')
        self._refresh_run_btn()

    # ── File browsing & validation ─────────────────────────────────────────────

    def _browse_file(self):
        path = filedialog.askopenfilename(
            title='Select Ticker CSV File',
            filetypes=[('CSV / text files', '*.csv *.txt'), ('All files', '*.*')],
        )
        if not path:
            return
        self._file_var.set(path)
        ok, msg, tickers = validate_ticker_file(path)
        if ok:
            self.tickers = tickers
            self._file_status.config(text=f'✓  {msg}', fg=C_GREEN)
        else:
            self.tickers = []
            self._file_status.config(text=f'✗  {msg}', fg=C_RED)
        self._refresh_run_btn()

    def _browse_output(self):
        path = filedialog.askdirectory(title='Select Output Folder')
        if path:
            self._out_var.set(path)

    # ── Run button state ──────────────────────────────────────────────────────

    def _refresh_run_btn(self):
        ready = self.tickers and self.bloomberg_ok and not self.analysis_active
        self._run_btn.config(
            state='normal' if ready else 'disabled',
            bg=C_BTN if ready else C_BTN_OFF,
        )

    # ── Start analysis ────────────────────────────────────────────────────────

    def _start_analysis(self):
        if not self.tickers:
            messagebox.showwarning('No Tickers', 'Please select a valid ticker file.')
            return
        if not self.bloomberg_ok:
            messagebox.showwarning('Bloomberg Not Running',
                                   'Bloomberg Terminal must be open before running analysis.')
            return

        output_dir = self._out_var.get()
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as exc:
            messagebox.showerror('Output Folder Error',
                                 f'Cannot create output folder:\n{exc}')
            return

        self.analysis_active = True
        self._refresh_run_btn()
        self._run_btn.config(text='⏳  Running…')
        self._prog_bar['value'] = 0
        self._prog_lbl.config(text='Starting…')

        threading.Thread(
            target=run_analysis,
            args=(list(self.tickers), output_dir, self.msg_q),
            daemon=True,
        ).start()

    # ── Queue polling ─────────────────────────────────────────────────────────

    def _poll_queue(self):
        try:
            while True:
                msg = self.msg_q.get_nowait()
                kind = msg[0]
                if kind == 'log':
                    _, level, text = msg
                    self._append_log(level, text)
                elif kind == 'progress':
                    _, pct, status = msg
                    self._prog_bar['value'] = pct
                    self._prog_lbl.config(text=status)
                elif kind == 'done':
                    _, success, summary = msg
                    self._on_done(success, summary)
        except queue.Empty:
            pass
        self.root.after(GUI_POLL_MS, self._poll_queue)

    def _append_log(self, level: str, text: str):
        self._log.config(state='normal')
        self._log.insert('end', text + '\n', level)
        self._log.see('end')
        self._log.config(state='disabled')

    def _on_done(self, success: bool, summary: str):
        self.analysis_active = False
        self._run_btn.config(text='▶   Run Analysis')
        self._refresh_run_btn()
        if success:
            self._prog_bar['value'] = 100
            self._prog_lbl.config(text='✓  Complete')
            messagebox.showinfo('Analysis Complete', summary)
        else:
            self._prog_lbl.config(text='✗  Failed')
            messagebox.showerror('Analysis Failed', summary)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    root = tk.Tk()
    root.geometry('720x660')
    try:
        # Set taskbar icon if bundled by PyInstaller
        icon = os.path.join(os.path.dirname(__file__), 'icon.ico')
        if os.path.exists(icon):
            root.iconbitmap(icon)
    except Exception:
        pass
    SquareGateApp(root)
    root.mainloop()


if __name__ == '__main__':
    main()
