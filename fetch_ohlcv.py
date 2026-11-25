"""Fetch 3m OHLCV from TradingView for several symbols.

First run per symbol: backfill up to 5000 bars.
Later runs: fetch the last few bars, merge by timestamp, save.
Only CLOSED candles are saved (the forming last bar is dropped).
"""
import os
import time

import pandas as pd
from tvDatafeed import TvDatafeed, Interval

# (symbol, exchange) -- edit this list
SYMBOLS = [
    ("XAUUSD", "OANDA"),
    ("BTCUSDT", "BINANCE"),
    ("NIFTY", "NSE"),
]
INTERVAL = Interval.in_3_minute
TAG = "3m"
BACKFILL_BARS = 5000
UPDATE_BARS = 100      # enough to catch up after skipped runs
MAX_ROWS = 20000       # keep the CSVs small
DATA_DIR = "data"

user, pwd = os.getenv("TV_USER"), os.getenv("TV_PASS")
tv = TvDatafeed(user, pwd) if user and pwd else TvDatafeed()


def fetch(symbol, exchange, n_bars, retries=3):
    for attempt in range(1, retries + 1):
        try:
            df = tv.get_hist(symbol, exchange, INTERVAL, n_bars=n_bars)
            if df is not None and len(df) > 0:
                return df.drop(columns=["symbol"], errors="ignore")
        except Exception as e:  # network / websocket hiccups
            print(f"  {symbol}: attempt {attempt} failed: {e}")
        time.sleep(2 * attempt)
    return None


def update(symbol, exchange):
    path = os.path.join(DATA_DIR, f"{symbol}_{TAG}.csv")
    have_file = os.path.exists(path)
    new = fetch(symbol, exchange, UPDATE_BARS if have_file else BACKFILL_BARS)
    if new is None:
        print(f"{symbol}: no data returned, skipped")
        return
    new = new.iloc[:-1]  # drop the still-forming candle
    if have_file:
        old = pd.read_csv(path, index_col=0, parse_dates=True)
        df = pd.concat([old, new])
    else:
        df = new
    df = df[~df.index.duplicated(keep="last")].sort_index().tail(MAX_ROWS)
    df.to_csv(path)
    print(f"{symbol}: {len(df)} bars, last closed {df.index[-1]}")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    for sym, exch in SYMBOLS:
        update(sym, exch)
        time.sleep(2)  # be gentle with TradingView
