#!/usr/bin/env python3
"""
MACD PCO / NCO scanner  ->  Google Sheet
=========================================
Fetches candles for every symbol on your symbol pages, calculates MACD (12, 26, 9) on
1m, 5m, 15m, 1H, 4H, 1D and 1W, finds the positive crossovers (PCO) and negative
crossovers (NCO), and writes two tabs into a Google Sheet:

  * "Scanner"  - one row per symbol, one column per timeframe (PCO / NCO / -)
  * "Signals"  - only the symbols that have a PCO or NCO, with the timeframes listed

There is no 40-request limit here, so every symbol and every timeframe is scanned.

ONE-TIME SETUP
--------------
1. pip install pandas yfinance gspread
2. Google Cloud Console -> create a project -> enable "Google Sheets API" and "Google Drive API".
3. IAM & Admin -> Service Accounts -> create one -> Keys -> Add key -> JSON.
   Save the file next to this script as  service_account.json
4. Create an empty Google Sheet, click Share, and share it (Editor) with the service
   account's e-mail (client_email inside the JSON file).
5. Copy the Sheet ID from its URL (the long text between /d/ and /edit) into SPREADSHEET_ID below.

RUN
---
  python macd_to_google_sheet.py --dry-run          # test: prints the table, no Google needed
  python macd_to_google_sheet.py                    # scan once and update the sheet
  python macd_to_google_sheet.py --loop 10          # scan every 10 minutes (09:00-23:30 IST, Mon-Fri)
  python macd_to_google_sheet.py --pages "Nifty Bank,IT"   # only some symbol pages

DATA SOURCE
-----------
By default candles come from Yahoo Finance (free, via the yfinance package):
  * works for NSE stocks and some NSE indices; data can be delayed
  * does NOT have MCX symbols (they show "n/a")  -> see fetch_closes_custom() to plug in a broker API
  * 1m data is limited by Yahoo to the last 7 days, which is enough for MACD
Some symbols may not exist on Yahoo; they show "n/a" and are listed in the console.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
from zoneinfo import ZoneInfo

import pandas as pd

# ============================== CONFIG ==============================
# On your own PC: paste the Sheet ID here and keep service_account.json next to the script.
# In the cloud (GitHub Actions): set the secrets SPREADSHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON instead.
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "PASTE_YOUR_GOOGLE_SHEET_ID_HERE")
SERVICE_ACCOUNT_FILE = "service_account.json"

TIMEFRAMES = ["1m", "5m", "15m", "1H", "4H", "1D", "1W"]
FAST, SLOW, SIGNAL = 12, 26, 9
LOOKBACK = 1               # a crossover on any of the last N bars counts (1 = latest bar only)
INCLUDE_FORMING_BAR = True  # True: the bar that is still forming counts. False: closed bars only
PAGES_TO_SCAN = None        # None = all pages, or e.g. ["Nifty Bank", "IT"]

IST = ZoneInfo("Asia/Kolkata")
LOOP_WINDOW = (dt.time(9, 0), dt.time(23, 30))  # only used with --loop; Mon-Fri

# timeframe label -> (yahoo interval, yahoo period, resample rule or None)
INTERVALS = {
    "1m":  ("1m",  "7d",   None),
    "5m":  ("5m",  "30d",  None),
    "15m": ("15m", "30d",  None),
    "1H":  ("60m", "90d",  None),
    "4H":  ("60m", "180d", "4h"),   # built from hourly candles: 09:15-13:15 and 13:15-close
    "1D":  ("1d",  "2y",   None),
    "1W":  ("1wk", "5y",   None),
}

# TradingView symbol name -> Yahoo Finance ticker (only the ones that differ from NAME.NS)
YAHOO_OVERRIDES = {
    "M_M": "M&M.NS",
    "NIFTY": "^NSEI", "CNXNIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK",
    "CNXAUTO": "^CNXAUTO", "CNXPHARMA": "^CNXPHARMA", "CNXMETAL": "^CNXMETAL",
    "CNXREAL": "^CNXREALTY", "CNXENER": "^CNXENERGY", "CNXINFR": "^CNXINFRA",
    "CNXPSE": "^CNXPSE", "CNXSERV": "^CNXSERVICE", "CNXFMCC": "^CNXFMCG",
}
# NSE index symbols from your pages that have no Yahoo ticker in the list above
INDEX_NAMES = {"MIDCPNIFTY", "CNXHEAL", "NIFTYPSU", "NIFTY_IN", "NIFTYPV", "CNXCOM", "CNXCONS", "NIFTY_C"}

DOWNLOAD_CHUNK = 40  # tickers per Yahoo request

# ============================== SYMBOL PAGES ==============================
# Same lists as f_getSymbols() in your Pine Script.
SYMBOLS = {
    "Nifty 50 (1-40)": [
        "NSE:RELIANCE", "NSE:TCS", "NSE:HDFCBANK", "NSE:ICICIBANK", "NSE:INFY", "NSE:HINDUNILVR", "NSE:ITC",
        "NSE:SBIN", "NSE:BHARTIARTL", "NSE:KOTAKBANK", "NSE:LT", "NSE:AXISBANK", "NSE:ASIANPAINT",
        "NSE:MARUTI", "NSE:HCLTECH", "NSE:SUNPHARMA", "NSE:TITAN", "NSE:ULTRACEMCO", "NSE:BAJFINANCE",
        "NSE:BSE", "NSE:NESTLEIND", "NSE:NTPC", "NSE:POWERGRID", "NSE:M_M", "NSE:TMPV", "NSE:TECHM",
        "NSE:ONGC", "NSE:JSWSTEEL", "NSE:TATASTEEL", "NSE:ADANIENT", "NSE:COALINDIA", "NSE:BAJAJFINSV",
        "NSE:HDFCLIFE", "NSE:SBILIFE", "NSE:GRASIM", "NSE:HINDALCO", "NSE:CIPLA", "NSE:DRREDDY",
        "NSE:EICHERMOT", "NSE:BAJAJ_AUTO",
    ],
    "Nifty 50 (41-50)": [
        "NSE:ADANIPORTS", "NSE:APOLLOHOSP", "NSE:BEL", "NSE:ETERNAL", "NSE:INDIGO", "NSE:JIOFIN",
        "NSE:MAXHEALTH", "NSE:SHRIRAMFIN", "NSE:TATACONSUM", "NSE:TRENT",
    ],
    "Indices": [
        "NSE:NIFTY", "NSE:CNXNIFTY", "NSE:MIDCPNIFTY", "NSE:BANKNIFTY", "NSE:CNXAUTO", "NSE:CNXPHARMA",
        "NSE:CNXHEAL", "NSE:CNXMETAL", "NSE:NIFTYPSU", "NSE:NIFTY_IN", "NSE:NIFTYPV", "NSE:CNXINFR",
        "NSE:CNXSERV", "NSE:CNXCOM", "NSE:CNXCONS", "NSE:CNXREAL", "NSE:CNXENER", "NSE:CNXPSE",
        "NSE:NIFTY_C", "NSE:CNXFMCC",
    ],
    "Nifty Midcap": [
        "NSE:ESCORTS", "NSE:PETRONET", "NSE:POLYCAB", "NSE:TORNTPOWER", "NSE:TRENT", "NSE:UBL", "NSE:ZEEL",
        "NSE:JUBLFOOD", "NSE:AUROPHARMA", "NSE:ASHOKLEY", "NSE:BHARATFORG", "NSE:CONCOR", "NSE:CUMMINSIND",
        "NSE:FEDERALBNK", "NSE:GMRAIRPORT", "NSE:GODREJPROP", "NSE:IDEA", "NSE:IDFCFIRSTB", "NSE:INDHOTEL",
        "NSE:INDUSINDBK",
    ],
    "Nifty Bank": [
        "NSE:HDFCBANK", "NSE:ICICIBANK", "NSE:SBIN", "NSE:AXISBANK", "NSE:BANKBARODA", "NSE:KOTAKBANK",
        "NSE:CANBK", "NSE:YESBANK", "NSE:IDFCFIRSTB", "NSE:FEDERALBNK", "NSE:PNB", "NSE:UNIONBANK",
        "NSE:INDUSINDBK", "NSE:AUBANK",
    ],
    "Nifty Auto": [
        "NSE:M_M", "NSE:ASHOKLEY", "NSE:TVSMOTOR", "NSE:MARUTI", "NSE:BAJAJ_AUTO", "NSE:HEROMOTOCO",
        "NSE:TIINDIA", "NSE:TMPV", "NSE:EICHERMOT", "NSE:BHARATFORG", "NSE:MOTHERSON", "NSE:SONACOMS",
        "NSE:EXIDEIND", "NSE:BOSCHLTD", "NSE:UNOMINDA",
    ],
    "Nifty Pharma": [
        "NSE:CIPLA", "NSE:ZYDUSLIFE", "NSE:BIOCON", "NSE:SUNPHARMA", "NSE:LAURUSLABS", "NSE:LUPIN",
        "NSE:DRREDDY", "NSE:DIVISLAB", "NSE:AUROPHARMA", "NSE:GLENMARK", "NSE:WOCKPHARMA", "NSE:PPLPHARMA",
        "NSE:AJANTPHARM", "NSE:TORNTPHARM", "NSE:JBCHEPHARM", "NSE:ABBOTINDIA", "NSE:MANKIND", "NSE:ALKEM",
        "NSE:IPCALAB", "NSE:GLAND",
    ],
    "Healthcare": [
        "NSE:ZYDUSLIFE", "NSE:SUNPHARMA", "NSE:APOLLOHOSP", "NSE:LAURUSLABS", "NSE:LUPIN", "NSE:BIOCON",
        "NSE:DIVISLAB", "NSE:DRREDDY", "NSE:GLENMARK", "NSE:AUROPHARMA", "NSE:MAXHEALTH", "NSE:PPLPHARMA",
        "NSE:FORTIS", "NSE:TORNTPHARM", "NSE:SYNGENE", "NSE:MANKIND", "NSE:ALKEM", "NSE:IPCALAB",
        "NSE:ABBOTINDIA",
    ],
    "Metal": [
        "NSE:VEDL", "NSE:SAIL", "NSE:HINDCOPPER", "NSE:TATASTEEL", "NSE:HINDALCO", "NSE:HINDZINC",
        "NSE:NMDC", "NSE:NATIONALUM", "NSE:JSWSTEEL", "NSE:JINDALSTEL", "NSE:LLOYDSME", "NSE:WELCORP",
        "NSE:APLAPOL", "NSE:JSL",
    ],
    "PSU Bank": [
        "NSE:SBIN", "NSE:CANBK", "NSE:BANKBARODA", "NSE:UNIONBANK", "NSE:INDIANB", "NSE:PNB", "NSE:BANKIND",
        "NSE:MAHABANK", "NSE:CENTRAL", "NSE:UCOBANK", "NSE:IOB", "NSE:PSB",
    ],
    "Defence": [
        "NSE:MTARTECH", "NSE:HAL", "NSE:GRSE", "NSE:SOLARINDS", "NSE:DATAPATTNS", "NSE:BEL", "NSE:MAZDOCK",
        "NSE:COCHINSHIP", "NSE:PARAS", "NSE:APOLLOHOSP", "NSE:BHARATFORG", "NSE:BDL", "NSE:MIDHANI",
        "NSE:AEQUUS", "NSE:ZENTEC", "NSE:DYNAMATIC", "NSE:BEML", "NSE:ASTRAMICRO", "NSE:AXISCADES",
    ],
    "Private Bank": [
        "NSE:HDFCBANK", "NSE:ICICIBANK", "NSE:AXISBANK", "NSE:KOTAKBANK", "NSE:IDFCFIRSTB", "NSE:YESBANK",
        "NSE:FEDERALBNK", "NSE:BANDHANBNK", "NSE:RBLBANK", "NSE:INDUSINDBK",
    ],
    "Infra": [
        "NSE:POLYCAB", "NSE:TORNTPOWER", "NSE:BHARTIARTL", "NSE:RELIANCE", "NSE:LT", "NSE:ONGC",
        "NSE:ADANIGREEN", "NSE:ASHOKLEY", "NSE:ADANIPORTS", "NSE:GRASIM", "NSE:HINDPETRO", "NSE:DLF",
        "NSE:POWERGRID", "NSE:INDUSTOWER", "NSE:APOLLOHOSP", "NSE:SUZLON", "NSE:BPCL", "NSE:INDIGO",
        "NSE:NTPC", "NSE:TATAPOWER",
    ],
    "Service Sector": [
        "NSE:BHARTIARTL", "NSE:HDFCBANK", "NSE:BSE", "NSE:ICICIBANK", "NSE:SBIN", "NSE:INFY", "NSE:TCS",
        "NSE:SHRIRAMFIN", "NSE:AXISBANK", "NSE:ETERNAL", "NSE:HCLTECH", "NSE:ADANIPORTS", "NSE:BAJFINANCE",
        "NSE:POWERGRID", "NSE:JIOFIN", "NSE:APOLLOHOSP", "NSE:KOTAKBANK", "NSE:WIPRO", "NSE:INDIGO",
        "NSE:NTPC",
    ],
    "Commodities": [
        "MCX:GOLD1!", "MCX:SILVER1!", "MCX:CRUDEOIL1!", "MCX:NATURALGAS1!", "MCX:COPPER1!",
        "MCX:ALUMINIUM1!", "MCX:ZINC1!", "MCX:LEAD1!", "MCX:NICKEL1!", "MCX:COTTON1!", "MCX:MENTHAOIL1!",
        "MCX:MCXBULLDEX1!", "MCX:MCXMETLDEX1!", "MCX:GOLDM1!", "MCX:SILVERM1!", "MCX:CRUDEOILM1!",
        "MCX:NATGASMINI1!", "MCX:ZINCMINI1!", "MCX:LEADMINI1!", "MCX:SILVERMIC1!",
    ],
    "Consumption": [
        "NSE:TRENT", "NSE:UBL", "NSE:JUBLFOOD", "NSE:BHARTIARTL", "NSE:DIXON", "NSE:ADANIPORTS",
        "NSE:ETERNAL", "NSE:M_M", "NSE:TVSMOTOR", "NSE:TITAN", "NSE:BAJAJ_AUTO", "NSE:DLF", "NSE:MARUTI",
        "NSE:APOLLOHOSP", "NSE:ITC", "NSE:HEROMOTOCO", "NSE:EICHERMOT", "NSE:INDIGO", "NSE:TATAPOWER",
        "NSE:ASIANPAINT",
    ],
    "Realty": [
        "NSE:DLF", "NSE:ANANTRAJ", "NSE:LODHA", "NSE:GODREJPROP", "NSE:PHOENIXLTD", "NSE:ABREL",
        "NSE:OBEROIRLTY", "NSE:PRESTIGE", "NSE:SOBHA", "NSE:BRIGADE",
    ],
    "Energy": [
        "NSE:TORNTPOWER", "NSE:NLCINDIA", "NSE:RELIANCE", "NSE:ADANIPORTS", "NSE:OIL", "NSE:ONGC",
        "NSE:BHEL", "NSE:ADANIGREEN", "NSE:ADANIENT", "NSE:COALINDIA", "NSE:HINDPETRO", "NSE:POWERINDC",
        "NSE:POWERGRID", "NSE:BPCL", "NSE:SUZLON", "NSE:SIEMENS", "NSE:NTPC", "NSE:TATAPOWER", "NSE:CGPOWER",
        "NSE:ABB",
    ],
    "PSE": [
        "NSE:HAL", "NSE:OIL", "NSE:ONGC", "NSE:BHEL", "NSE:COALINDIA", "NSE:HINDPETRO", "NSE:BEL",
        "NSE:NMDC", "NSE:POWERGRID", "NSE:BPCL", "NSE:PFC", "NSE:NTPC", "NSE:IOC", "NSE:RECLTD", "NSE:RVNL",
        "NSE:IRFC", "NSE:GAIL", "NSE:NHPC", "NSE:IRCTC",
    ],
    "Consumer Durables": [
        "NSE:POLYCAB", "NSE:DIXON", "NSE:TITAN", "NSE:KALYANKJIL", "NSE:CROMPTON", "NSE:AMBER", "NSE:PGEL",
        "NSE:BLUESTARCO", "NSE:VOLTAS", "NSE:LGEINDIA", "NSE:HAVELLS", "NSE:WHIRLPOOL", "NSE:KAJARIACER",
    ],
    "FMCG": [
        "NSE:UBL", "NSE:JUBLFOOD", "NSE:ITC", "NSE:HINDUNILVR", "NSE:VBL", "NSE:TATACONSUM", "NSE:BRITANNIA",
        "NSE:RADICO", "NSE:NESTLEIND", "NSE:GODREJCP", "NSE:DABUR", "NSE:MARICO", "NSE:UNITDSPR",
        "NSE:PATANJALI", "NSE:COLPAL",
    ],
    "Oil & Gas": [
        "NSE:PETRONET", "NSE:RELIANCE", "NSE:OIL", "NSE:ONGC", "NSE:HINDPETRO", "NSE:BPCL", "NSE:IOC",
        "NSE:GAIL", "NSE:ATGL", "NSE:CHENNPETRO", "NSE:AEGISLOG", "NSE:MGL", "NSE:IGL", "NSE:CASTROLIND",
    ],
    "IT": [
        "NSE:INFY", "NSE:TCS", "NSE:HCLTECH", "NSE:COFORGE", "NSE:PERSISTENT", "NSE:WIPRO", "NSE:LTIM",
        "NSE:TECHM", "NSE:MPHASIS", "NSE:OFSS",
    ],
}


# ============================== SYMBOL MAPPING ==============================
def to_yahoo(tv_symbol):
    """'NSE:RELIANCE' -> 'RELIANCE.NS'. Returns None if Yahoo has no such symbol (e.g. MCX)."""
    exchange, _, name = tv_symbol.partition(":")
    if exchange != "NSE":
        return None
    if name in YAHOO_OVERRIDES:
        return YAHOO_OVERRIDES[name]
    if name in INDEX_NAMES:
        return None
    return name.replace("_", "-") + ".NS"


# ============================== DATA ==============================
def fetch_closes_custom(tv_symbols, tf):
    """
    PLUG-IN POINT for a broker API (Zerodha Kite, Upstox, Angel One, Dhan ...), needed for MCX.
    Return {tv_symbol: pandas.Series of closes (oldest -> newest)} for the timeframe label `tf`.
    Leave it returning {} to use Yahoo only.
    """
    return {}


def fetch_closes_yahoo(tv_symbols, tf):
    import yfinance as yf

    interval, period, resample = INTERVALS[tf]
    ymap = {s: to_yahoo(s) for s in tv_symbols}
    tickers = sorted({y for y in ymap.values() if y})
    closes_by_ticker = {}

    for i in range(0, len(tickers), DOWNLOAD_CHUNK):
        chunk = tickers[i:i + DOWNLOAD_CHUNK]
        try:
            data = yf.download(chunk, period=period, interval=interval, group_by="ticker",
                               auto_adjust=False, progress=False, threads=True)
        except Exception as exc:  # network / Yahoo problem: skip this chunk
            print(f"  ! download failed for {tf} chunk {i // DOWNLOAD_CHUNK + 1}: {exc}")
            continue
        for y in chunk:
            try:
                frame = data[y] if isinstance(data.columns, pd.MultiIndex) else data
                series = frame["Close"].dropna()
            except Exception:
                continue
            if series.empty:
                continue
            if resample:
                if series.index.tz is None:
                    series.index = series.index.tz_localize(IST)
                else:
                    series = series.tz_convert(IST)
                series = series.resample(resample, origin="start_day", offset="9h15min").last().dropna()
            closes_by_ticker[y] = series

    return {s: closes_by_ticker[y] for s, y in ymap.items() if y and y in closes_by_ticker}


def fetch_closes(tv_symbols, tf):
    result = fetch_closes_yahoo(tv_symbols, tf)
    custom = fetch_closes_custom([s for s in tv_symbols if s not in result], tf)
    result.update(custom)
    return result


# ============================== MACD ==============================
def macd_state(close, lookback=LOOKBACK, include_forming=INCLUDE_FORMING_BAR):
    """'PCO', 'NCO', '-' (no crossover) or None (not enough data)."""
    offset = 0 if include_forming else 1
    if close is None or len(close) < SLOW + SIGNAL + lookback + offset + 5:
        return None
    ema_fast = close.ewm(span=FAST, adjust=False).mean()
    ema_slow = close.ewm(span=SLOW, adjust=False).mean()
    macd = (ema_fast - ema_slow).to_numpy()
    sig = pd.Series(macd).ewm(span=SIGNAL, adjust=False).mean().to_numpy()
    n = len(macd)
    for i in range(offset, offset + lookback):
        a = n - 1 - i  # position of the bar i bars ago
        if macd[a] > sig[a] and macd[a - 1] <= sig[a - 1]:
            return "PCO"
        if macd[a] < sig[a] and macd[a - 1] >= sig[a - 1]:
            return "NCO"
    return "-"


# ============================== SCAN ==============================
def scan(pages):
    unique = sorted({s for syms in pages.values() for s in syms})
    states = {}  # (symbol, timeframe) -> 'PCO' / 'NCO' / '-' / 'n/a'
    for tf in TIMEFRAMES:
        print(f"Fetching {tf} candles for {len(unique)} symbols ...")
        closes = fetch_closes(unique, tf)
        for s in unique:
            state = macd_state(closes.get(s))
            states[(s, tf)] = state if state else "n/a"
        missing = [s for s in unique if states[(s, tf)] == "n/a"]
        if missing:
            print(f"  no data for {len(missing)} symbols on {tf}: {', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}")
    return states


def build_tables(pages, states):
    now = dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    header = ["Page", "Symbol"] + TIMEFRAMES + ["PCO count", "NCO count", "Updated"]
    matrix = [header]
    per_symbol = {}  # symbol -> {"pages": [...], "PCO": [...], "NCO": [...]}

    for page, syms in pages.items():
        for s in syms:
            row_states = [states[(s, tf)] for tf in TIMEFRAMES]
            pco = [tf for tf, st in zip(TIMEFRAMES, row_states) if st == "PCO"]
            nco = [tf for tf, st in zip(TIMEFRAMES, row_states) if st == "NCO"]
            matrix.append([page, s.split(":", 1)[-1]] + row_states + [len(pco), len(nco), now])
            info = per_symbol.setdefault(s, {"pages": [], "PCO": pco, "NCO": nco})
            info["pages"].append(page)

    signals = [["Stock Symbol", "PCO timeframes", "NCO timeframes", "Pages", "Updated"]]
    ordered = sorted(per_symbol.items(), key=lambda kv: (-(len(kv[1]["PCO"]) + len(kv[1]["NCO"])), kv[0]))
    for s, info in ordered:
        if info["PCO"] or info["NCO"]:
            signals.append([s.split(":", 1)[-1], ", ".join(info["PCO"]), ", ".join(info["NCO"]),
                            ", ".join(info["pages"]), now])
    return matrix, signals


# ============================== GOOGLE SHEET ==============================
def _get_ws(sh, title, rows, cols):
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def _ensure_formatting(sh, ws):
    """Green for PCO, red for NCO in the timeframe columns of the Scanner tab (added once)."""
    meta = sh.fetch_sheet_metadata()
    for sheet in meta["sheets"]:
        if sheet["properties"]["sheetId"] == ws.id and sheet.get("conditionalFormats"):
            return
    rng = {"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 2 + len(TIMEFRAMES)}

    def rule(text, rgb):
        return {"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [rng],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": text}]},
                            "format": {"backgroundColor": {"red": rgb[0], "green": rgb[1], "blue": rgb[2]}}}}}}

    sh.batch_update({"requests": [rule("PCO", (0.72, 0.88, 0.72)), rule("NCO", (0.96, 0.72, 0.72))]})


def write_google_sheet(matrix, signals):
    import gspread

    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if creds_json:
        gc = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws = _get_ws(sh, "Scanner", len(matrix) + 20, len(matrix[0]) + 2)
    ws.clear()
    ws.update(values=matrix, range_name="A1")
    ws.freeze(rows=1)
    _ensure_formatting(sh, ws)

    ws2 = _get_ws(sh, "Signals", max(len(signals) + 20, 100), len(signals[0]) + 2)
    ws2.clear()
    ws2.update(values=signals, range_name="A1")
    ws2.freeze(rows=1)
    print(f"Google Sheet updated: {len(matrix) - 1} rows in Scanner, {len(signals) - 1} symbols in Signals.")


# ============================== MAIN ==============================
def run_once(dry_run, page_names):
    names = page_names or PAGES_TO_SCAN or list(SYMBOLS)
    unknown = [n for n in names if n not in SYMBOLS]
    if unknown:
        sys.exit(f"Unknown page(s): {unknown}. Available: {list(SYMBOLS)}")
    pages = {n: SYMBOLS[n] for n in names}

    states = scan(pages)
    matrix, signals = build_tables(pages, states)

    if dry_run:
        print()
        print(pd.DataFrame(signals[1:], columns=signals[0]).to_string(index=False) if len(signals) > 1
              else "No PCO / NCO found.")
        print(f"\n{len(signals) - 1} symbols have a PCO or NCO. (dry run: Google Sheet not touched)")
    else:
        write_google_sheet(matrix, signals)


def in_window(now):
    return now.weekday() < 5 and LOOP_WINDOW[0] <= now.time() <= LOOP_WINDOW[1]


def main():
    ap = argparse.ArgumentParser(description="MACD PCO/NCO scanner -> Google Sheet")
    ap.add_argument("--dry-run", action="store_true", help="print the result, do not write to Google Sheets")
    ap.add_argument("--loop", type=int, metavar="MINUTES", help="repeat every N minutes")
    ap.add_argument("--ignore-hours", action="store_true", help="with --loop: also run outside market hours")
    ap.add_argument("--pages", help='comma separated page names, e.g. "Nifty Bank,IT"')
    args = ap.parse_args()
    page_names = [p.strip() for p in args.pages.split(",")] if args.pages else None

    if not args.dry_run and SPREADSHEET_ID.startswith("PASTE_"):
        sys.exit("Set SPREADSHEET_ID at the top of the script first (or use --dry-run).")

    if not args.loop:
        run_once(args.dry_run, page_names)
        return

    while True:
        now = dt.datetime.now(IST)
        if args.ignore_hours or in_window(now):
            try:
                run_once(args.dry_run, page_names)
            except Exception as exc:  # keep the loop alive on temporary errors
                print(f"Run failed: {exc}")
        else:
            print(f"{now:%H:%M} IST - outside the scan window, waiting ...")
        time.sleep(args.loop * 60)


if __name__ == "__main__":
    main()
