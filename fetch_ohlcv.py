"""Fetch 3m OHLCV from TradingView (times in IST) and send a Telegram
timing alert whenever new closed 3m bars arrive.

First run per symbol: backfill 5000 bars (cold start).
Later runs: fetch only the last few bars and append the ones newer than
what is already saved. If the fetched window does not overlap the saved
data (a gap), it fetches more bars automatically to fill the hole.
Only CLOSED candles are saved. Full history is kept (no trimming).

Alert = one Telegram message per run (only if there are new bars) with:
bar open/close time (IST), when the alert was sent, and the lag between
bar close and alert. Every alert is also logged to data/alert_log.csv.
"""
import os
import time

# tvDatafeed turns timestamps into the machine's local time, so pin the
# machine to UTC first; then we convert to IST ourselves.
os.environ["TZ"] = "UTC"
time.tzset()

import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval

# (symbol, exchange) -- edit this list
SYMBOLS = [
    ("XAUUSD", "OANDA"),
    ("BTCUSDT", "BINANCE"),
    ("NIFTY", "NSE"),
]
INTERVAL = Interval.in_3_minute
TAG = "3m"
BAR_MIN = 3
TZ = "Asia/Kolkata"
BACKFILL_BARS = 5000
UPDATE_STEPS = (15, 200, BACKFILL_BARS)  # widen the window only if needed
DATA_DIR = "data"
ALERT_LOG = os.path.join(DATA_DIR, "alert_log.csv")

STARTED = pd.Timestamp.now(tz=TZ)
EVENT = os.getenv("GITHUB_EVENT_NAME", "manual")

user, pwd = os.getenv("TV_USER"), os.getenv("TV_PASS")
tv = TvDatafeed(user, pwd) if user and pwd else TvDatafeed()


# ---------------------------------------------------------------- data
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
    """Returns None (nothing new) or a dict describing what was added."""
    path = os.path.join(DATA_DIR, f"{symbol}_{TAG}.csv")

    if not os.path.exists(path):  # cold start
        df = fetch(symbol, exchange, BACKFILL_BARS)
        if df is None:
            print(f"{symbol}: no data returned, skipped")
            return None
        df = df[~df.index.duplicated(keep="last")].sort_index()
        df.to_csv(path)
        print(f"{symbol}: backfilled {len(df)} bars, last {df.index[-1]}")
        return {"kind": "backfill", "n": len(df)}

    old = load(path)
    last = old.index[-1]
    raw = None
    for n in UPDATE_STEPS:
        raw = fetch(symbol, exchange, n)
        if raw is None:
            print(f"{symbol}: no data returned, skipped")
            return None
        if raw.index[0] <= last:  # window overlaps saved data: no gap
            break
        print(f"  {symbol}: possible gap, widening window beyond {n} bars")

    new = raw[raw.index > last]
    if new.empty:
        old.to_csv(path)  # also converts older files to IST
        print(f"{symbol}: no new bars (last {last})")
        return None
    df = pd.concat([old, new])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_csv(path)
    print(f"{symbol}: +{len(new)} bars, total {len(df)}, last {df.index[-1]}")
    return {"kind": "new", "n": len(new), "open": new.index[-1]}


# --------------------------------------------------------------- alerts
def send_telegram(text):
    token, chat = os.getenv("TG_TOKEN"), os.getenv("TG_CHAT_ID")
    if not token or not chat:
        print("Telegram not configured (TG_TOKEN / TG_CHAT_ID), alert not sent")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": text},
            timeout=15,
        )
        if r.status_code != 200:
            # never print the URL or the exception text: they contain the token
            try:
                why = r.json().get("description", "")
            except Exception:
                why = ""
            print(f"Telegram error: HTTP {r.status_code} {why}")
            return False
        return True
    except Exception as e:
        print(f"Telegram send failed: {type(e).__name__}")
        return False


def notify(results):
    if not results:
        print("No new bars, no alert.")
        return
    now = pd.Timestamp.now(tz=TZ)
    lines = [f"3m bar alert | {now:%d %b %H:%M:%S} IST"]
    if EVENT == "schedule":
        late = STARTED.minute % 5 + STARTED.second / 60
        lines.append(f"Run started {STARTED:%H:%M:%S} (~{late:.1f} min after the 5-min mark)")
    else:
        lines.append(f"Run started {STARTED:%H:%M:%S} ({EVENT} run)")
    lines.append("")

    rows = []
    for sym, r in results.items():
        if r["kind"] == "backfill":
            lines.append(f"{sym}: cold start, saved {r['n']} bars")
            continue
        close = r["open"] + pd.Timedelta(minutes=BAR_MIN)
        lag = (now - close).total_seconds() / 60
        what = f"+{r['n']} bars, newest" if r["n"] > 1 else "bar"
        lines.append(f"{sym}: {what} {r['open']:%H:%M}-{close:%H:%M} | lag {lag:.1f} min")
        rows.append({
            "alert_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": sym,
            "bar_open_ist": r["open"].strftime("%Y-%m-%d %H:%M:%S"),
            "bar_close_ist": close.strftime("%Y-%m-%d %H:%M:%S"),
            "lag_min": round(lag, 2),
            "new_bars": r["n"],
            "run_start_ist": STARTED.strftime("%Y-%m-%d %H:%M:%S"),
            "event": EVENT,
        })

    sent = send_telegram("\n".join(lines))
    if rows:
        out = pd.DataFrame(rows)
        out["sent"] = sent
        out.to_csv(ALERT_LOG, mode="a", header=not os.path.exists(ALERT_LOG), index=False)


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    if EVENT == "workflow_dispatch":  # manual run: confirm Telegram works
        send_telegram(f"Test: Telegram is connected. Run started {STARTED:%d %b %H:%M:%S} IST")
    results = {}
    for sym, exch in SYMBOLS:
        try:
            res = update(sym, exch)
            if res:
                results[sym] = res
        except Exception as e:
            print(f"{sym}: failed with {type(e).__name__}: {e}")
        time.sleep(2)  # be gentle with TradingView
    notify(results)
