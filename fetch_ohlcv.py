"""Fetch 3m OHLCV from TradingView (times in IST) for several symbols.

First run per symbol: backfill 5000 bars.
Later runs: fetch only the last few bars and append the ones newer than
what is already saved. If the fetched window does not overlap the saved
data (a gap), it automatically fetches more bars to fill the hole.
Only CLOSED candles are saved. Full history is kept (no trimming).
"""
import os
import time

# tvDatafeed turns timestamps into the machine's local time, so pin the
# machine to UTC first; then we convert to IST ourselves.
os.environ["TZ"] = "UTC"
time.tzset()

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
TZ = "Asia/Kolkata"
BACKFILL_BARS = 5000
UPDATE_STEPS = (15, 200, BACKFILL_BARS)  # widen the window only if needed
DATA_DIR = "data"

user, pwd = os.getenv("TV_USER"), os.getenv("TV_PASS")
tv = TvDatafeed(user, pwd) if user and pwd else TvDatafeed()


def fetch(symbol, exchange, n_bars, retries=3):
    for attempt in range(1, retries + 1):
        try:
            df = tv.get_hist(symbol, exchange, INTERVAL, n_bars=n_bars)
            if df is not None and len(df) > 1:
                df = df.drop(columns=["symbol"], errors="ignore")
                df.index = df.index.tz_localize("UTC").tz_convert(TZ)
                df.index.name = "datetime"
                return df.iloc[:-1]  # drop the still-forming candle
        except Exception as e:  # network / websocket hiccups
            print(f"  {symbol}: attempt {attempt} failed: {e}")
        time.sleep(2 * attempt)
    return None


def load(path):
    df = pd.read_csv(path, index_col=0)
    # works for old naive-UTC files and for files with a +05:30 offset
    df.index = pd.to_datetime(df.index, utc=True).tz_convert(TZ)
    df.index.name = "datetime"
    return df


def update(symbol, exchange):
    path = os.path.join(DATA_DIR, f"{symbol}_{TAG}.csv")

    if not os.path.exists(path):
        df = fetch(symbol, exchange, BACKFILL_BARS)
        if df is None:
            print(f"{symbol}: no data returned, skipped")
            return
        df = df[~df.index.duplicated(keep="last")].sort_index()
        df.to_csv(path)
        print(f"{symbol}: backfilled {len(df)} bars, last {df.index[-1]}")
        return

    old = load(path)
    last = old.index[-1]
    raw = None
    for n in UPDATE_STEPS:
        raw = fetch(symbol, exchange, n)
        if raw is None:
            print(f"{symbol}: no data returned, skipped")
            return
        if raw.index[0] <= last:  # window overlaps saved data: no gap
            break
        print(f"  {symbol}: possible gap, widening window beyond {n} bars")

    new = raw[raw.index > last]
    if new.empty:
        # still rewrite once so older files get converted to IST
        old.to_csv(path)
        print(f"{symbol}: no new bars (last {last})")
        return
    df = pd.concat([old, new])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_csv(path)
    print(f"{symbol}: +{len(new)} bars, total {len(df)}, last {df.index[-1]}")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    for sym, exch in SYMBOLS:
        update(sym, exch)
        time.sleep(2)  # be gentle with TradingView
