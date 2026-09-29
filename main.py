import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from alpaca.data.live.stock import StockDataStream
from alpaca.trading.client import TradingClient

from telegram_bot import telegram_poll_loop, send_message


# ============================================
# SETTINGS
# ============================================

TICKERS = [
    "AAPL", "AMD", "ASTS", "BE", "GOOG", "HIMS", "INTC",
    "IREN", "JPM", "MRVL", "NBIS", "NVDA", "ORCL", "PLTR",
    "QQQ", "RKLB", "SPCX", "SPY", "TSLA",
]

TIMEZONE = ZoneInfo("America/New_York")
START_TIME = pd.Timestamp("10:00").time()
CONTRACTS = 5
ORDERS_ENABLED = False
EMA_LENGTH = 9
VWAP_DISTANCE_THRESHOLD = 0.28


# ============================================
# ALPACA
# ============================================

API_KEY = os.environ["APCA_API_KEY_ID"]
API_SECRET = os.environ["APCA_API_SECRET_KEY"]
PAPER_MODE = os.environ.get("ALPACA_PAPER", "true").lower() == "true"

historical_client = StockHistoricalDataClient(API_KEY, API_SECRET)
trading_client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)

print("========================================")
print("OPTIONS TRADING BOT")
print("========================================")
print("Timeframe:       5 minutes")
print("Start time:      10:00 NY")
print("Data feed:       IEX")
print(f"Contracts:       {CONTRACTS}")
print(f"VWAP filter:     {VWAP_DISTANCE_THRESHOLD:.2f}%")
print(f"Orders:          {'ENABLED' if ORDERS_ENABLED else 'DISABLED'}")
print(f"Paper mode:      {'ENABLED' if PAPER_MODE else 'DISABLED'}")
print("========================================")


# ============================================
# TELEGRAM / SHARED STATE
# ============================================

five_minute_history = {ticker: [] for ticker in TICKERS}
one_minute_bars = {ticker: [] for ticker in TICKERS}
active_entries = {ticker: [] for ticker in TICKERS}
pending_signals = {ticker: None for ticker in TICKERS}

state = {
    "tickers": TICKERS,
    "paper_mode": PAPER_MODE,
    "online": True,
    "alpaca_connected": False,
    "last_market_update": None,
    "portfolio_equity": None,
    "history": five_minute_history,
    "active_entries": active_entries,
    "trades_today": [],
    "signals_today": [],
    "watching": {},
}


def update_portfolio_value():
    try:
        account = trading_client.get_account()
        state["portfolio_equity"] = float(account.equity)
        state["alpaca_connected"] = True
    except Exception as exc:
        state["alpaca_connected"] = False
        print(f"Could not read Alpaca account: {exc}")


def today_start():
    now = datetime.now(TIMEZONE)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


# ============================================
# HISTORICAL DATA
# ============================================

def load_today_history():
    now_ny = datetime.now(TIMEZONE)
    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    end_time = now_ny - timedelta(minutes=1)

    print()
    print("Loading today's historical IEX bars...")
    print(f"Start: {market_open}")
    print(f"End:   {end_time}")

    request = StockBarsRequest(
        symbol_or_symbols=TICKERS,
        timeframe=TimeFrame(1, TimeFrameUnit.Minute),
        start=market_open,
        end=end_time,
        feed=DataFeed.IEX,
        limit=10000,
    )

    bars = historical_client.get_stock_bars(request)
    df = bars.df.reset_index()

    if df.empty:
        raise RuntimeError("No historical bars were returned.")

    if "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("America/New_York")

    df = df.rename(columns={"timestamp": "time"})
    df = df[["ticker", "time", "open", "high", "low", "close", "volume"]]
    df = df.sort_values(["ticker", "time"]).reset_index(drop=True)

    print(f"Historical rows: {len(df):,}")
    return df


def prepare_strategy_data(raw_data):
    results = []

    for ticker in TICKERS:
        ticker_df = raw_data[raw_data["ticker"] == ticker].copy()
        if ticker_df.empty:
            continue

        ticker_df = ticker_df.sort_values("time").set_index("time")

        five = ticker_df.resample("5min", origin="start_day").agg({
            "ticker": "first",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna(subset=["open", "high", "low", "close"])

        five["ema9"] = five["open"].ewm(
            span=EMA_LENGTH, adjust=False
        ).mean()

        five["date"] = five.index.date
        five["open_volume"] = five["open"] * five["volume"]
        five["cumulative_open_volume"] = (
            five.groupby("date")["open_volume"].cumsum()
        )
        five["cumulative_volume"] = (
            five.groupby("date")["volume"].cumsum()
        )
        five["vwap"] = (
            five["cumulative_open_volume"] /
            five["cumulative_volume"]
        )

        results.append(five.reset_index())

    if not results:
        raise RuntimeError("No 5-minute strategy data created.")

    return pd.concat(results, ignore_index=True).sort_values(
        ["ticker", "time"]
    ).reset_index(drop=True)


# ============================================
# WATCH STATE
# ============================================

def update_watch_state(ticker, df):
    if len(df) == 0:
        return

    row = df.iloc[-1]
    price = float(row["close"])
    vwap = float(row["vwap"])
    ema9 = float(row["ema9"])

    if price > vwap:
        direction = "SHORT"
        waiting = "New candle must cross BELOW VWAP"
    elif price < vwap:
        direction = "LONG"
        waiting = "New candle must cross ABOVE VWAP"
    else:
        direction = "LONG / SHORT"
        waiting = "New candle must cross VWAP"

    pending = pending_signals.get(ticker)
    if pending:
        direction = pending["direction"]
        waiting = (
            f"Next candle OPEN must be "
            f"{'ABOVE' if direction == 'LONG' else 'BELOW'} "
            f"signal EMA9"
        )
        status = "PENDING ENTRY"
    else:
        status = "WAITING"

    state["watching"][ticker] = {
        "direction": direction,
        "price": price,
        "vwap": vwap,
        "ema9": ema9,
        "waiting_for": waiting,
        "status": status,
    }


# ============================================
# TRADE / SIGNAL RECORDS
# ============================================

def record_trade_entry(ticker, entry):
    state["trades_today"].append({
        "ticker": ticker,
        "direction": entry["direction"],
        "entry_time": entry["entry_time"],
        "entry_price": entry["entry_price"],
        "status": "OPEN",
        "return_pct": None,
    })


def close_trade_record(ticker, entry, exit_time, exit_price, reason):
    for trade in reversed(state["trades_today"]):
        if (
            trade["ticker"] == ticker
            and trade["status"] == "OPEN"
            and trade["entry_time"] == entry["entry_time"]
        ):
            entry_price = float(trade["entry_price"])

            if trade["direction"] == "LONG":
                pct = (exit_price / entry_price - 1) * 100
            else:
                pct = (entry_price / exit_price - 1) * 100

            trade["status"] = "CLOSED"
            trade["exit_time"] = exit_time
            trade["exit_price"] = exit_price
            trade["exit_reason"] = reason
            trade["return_pct"] = pct
            return


# ============================================
# ENTRY
# ============================================

def create_pending_signal(ticker, df):
    if len(df) < 3:
        return

    previous = df.iloc[-2]
    signal = df.iloc[-1]

    previous_time = pd.Timestamp(previous["time"])
    signal_time = pd.Timestamp(signal["time"])

    if previous_time.date() != signal_time.date():
        return

    if signal_time.time() < START_TIME:
        return

    long_cross = (
        previous["close"] <= previous["vwap"]
        and signal["close"] > signal["vwap"]
    )

    short_cross = (
        previous["close"] >= previous["vwap"]
        and signal["close"] < signal["vwap"]
    )

    if not long_cross and not short_cross:
        return

    direction = "LONG" if long_cross else "SHORT"

    close_price = float(signal["close"])
    vwap = float(signal["vwap"])
    ema9 = float(signal["ema9"])

    if vwap == 0:
        return

    distance = abs(close_price - vwap) / abs(vwap) * 100

    if distance < VWAP_DISTANCE_THRESHOLD:
        state["signals_today"].append({
            "ticker": ticker,
            "direction": direction,
            "time": signal_time,
            "vwap": vwap,
            "close": close_price,
            "ema9": ema9,
            "distance_pct": distance,
            "status": "REJECTED — VWAP distance",
        })
        return

    ema_valid = (
        close_price > ema9
        if direction == "LONG"
        else close_price < ema9
    )

    if not ema_valid:
        state["signals_today"].append({
            "ticker": ticker,
            "direction": direction,
            "time": signal_time,
            "vwap": vwap,
            "close": close_price,
            "ema9": ema9,
            "distance_pct": distance,
            "status": "REJECTED — signal EMA9",
        })
        return

    pending_signals[ticker] = {
        "direction": direction,
        "signal_time": signal_time,
        "signal_close": close_price,
        "signal_ema9": ema9,
        "vwap": vwap,
        "distance_pct": distance,
        "signal_index": len(state["signals_today"]),
    }

    state["signals_today"].append({
        "ticker": ticker,
        "direction": direction,
        "time": signal_time,
        "vwap": vwap,
        "close": close_price,
        "ema9": ema9,
        "distance_pct": distance,
        "status": "PENDING ENTRY",
    })

    print(
        f"{ticker} {direction} signal at {signal_time}; "
        "waiting for next candle open."
    )


def execute_pending_at_open(ticker, candle_open, candle_time):
    pending = pending_signals.get(ticker)

    if not pending:
        return

    direction = pending["direction"]
    signal_ema9 = pending["signal_ema9"]

    valid = (
        candle_open > signal_ema9
        if direction == "LONG"
        else candle_open < signal_ema9
    )

    if not valid:
        status = (
            "REJECTED — next candle opened below EMA9"
            if direction == "LONG"
            else "REJECTED — next candle opened above EMA9"
        )

        for signal in reversed(state["signals_today"]):
            if (
                signal["ticker"] == ticker
                and signal["time"] == pending["signal_time"]
            ):
                signal["status"] = status
                break

        pending_signals[ticker] = None
        return

    entry = {
        "direction": direction,
        "entry_time": candle_time,
        "entry_price": float(candle_open),
        "entry_ema9": signal_ema9,
    }

    active_entries[ticker].append(entry)
    record_trade_entry(ticker, entry)

    for signal in reversed(state["signals_today"]):
        if (
            signal["ticker"] == ticker
            and signal["time"] == pending["signal_time"]
        ):
            signal["status"] = "ENTERED"
            break

    pending_signals[ticker] = None

    print(
        f"ENTRY {ticker} {direction} at {candle_open:.4f} "
        f"on {candle_time}"
    )

    if ORDERS_ENABLED:
        # Option execution will be added only after signal verification.
        pass


# ============================================
# EXITS
# ============================================

def check_exits(ticker, candle, df):
    if not active_entries[ticker]:
        return

    current_time = pd.Timestamp(candle["time"])
    close = float(candle["close"])
    ema9 = float(df.iloc[-1]["ema9"])

    remaining = []

    for entry in active_entries[ticker]:
        if current_time <= entry["entry_time"]:
            remaining.append(entry)
            continue

        direction = entry["direction"]

        should_exit = (
            close < ema9 if direction == "LONG"
            else close > ema9
        )

        if should_exit:
            close_trade_record(
                ticker,
                entry,
                current_time,
                close,
                "EMA9",
            )
            print(
                f"EXIT {ticker} {direction} at {close:.4f} "
                f"reason=EMA9"
            )
        else:
            remaining.append(entry)

    active_entries[ticker] = remaining


def end_of_day_exit(ticker, candle):
    if not active_entries[ticker]:
        return

    current_time = pd.Timestamp(candle["time"])

    # Final regular-session 5-minute candle normally starts at 15:55.
    # We also force the last available completed candle at the session end.
    if not (
        (current_time.hour == 15 and current_time.minute == 55)
        or (current_time.hour >= 16)
    ):
        return

    close = float(candle["close"])

    for entry in active_entries[ticker]:
        close_trade_record(
            ticker,
            entry,
            current_time,
            close,
            "EOD",
        )

    print(f"EOD EXIT {ticker} at {close:.4f}")
    active_entries[ticker] = []


# ============================================
# LIVE BAR HANDLER
# ============================================

async def handle_bar(bar):
    ticker = bar.symbol

    if ticker not in one_minute_bars:
        return

    timestamp = pd.Timestamp(bar.timestamp)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    timestamp = timestamp.tz_convert("America/New_York")

    state["last_market_update"] = timestamp

    record = {
        "ticker": ticker,
        "time": timestamp,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
    }

    one_minute_bars[ticker].append(record)

    # At the OPEN of a new 5-minute candle, execute the previous
    # completed candle's pending signal using the actual candle open.
    if timestamp.minute % 5 == 0:
        execute_pending_at_open(
            ticker,
            float(bar.open),
            timestamp.floor("5min"),
        )

    one_minute_bars[ticker] = one_minute_bars[ticker][-5:]

    # Only build a completed 5-minute candle on minute 4.
    if timestamp.minute % 5 != 4:
        return

    bars = one_minute_bars[ticker]

    if len(bars) < 5:
        return

    bucket = timestamp.floor("5min")
    df_1m = pd.DataFrame(bars)

    if not all(
        pd.Timestamp(x).floor("5min") == bucket
        for x in df_1m["time"]
    ):
        return

    candle = {
        "ticker": ticker,
        "time": bucket,
        "open": float(df_1m.iloc[0]["open"]),
        "high": float(df_1m["high"].max()),
        "low": float(df_1m["low"].min()),
        "close": float(df_1m.iloc[-1]["close"]),
        "volume": float(df_1m["volume"].sum()),
    }

    five_minute_history[ticker].append(candle)
    five_minute_history[ticker] = five_minute_history[ticker][-1000:]

    strategy_df = pd.DataFrame(five_minute_history[ticker])

    if len(strategy_df) < 3:
        update_watch_state(ticker, strategy_df)
        return

    strategy_df["ema9"] = strategy_df["open"].ewm(
        span=EMA_LENGTH,
        adjust=False,
    ).mean()

    strategy_df["date"] = strategy_df["time"].dt.date
    strategy_df["open_volume"] = (
        strategy_df["open"] * strategy_df["volume"]
    )
    strategy_df["cumulative_open_volume"] = (
        strategy_df.groupby("date")["open_volume"].cumsum()
    )
    strategy_df["cumulative_volume"] = (
        strategy_df.groupby("date")["volume"].cumsum()
    )
    strategy_df["vwap"] = (
        strategy_df["cumulative_open_volume"] /
        strategy_df["cumulative_volume"]
    )

    # Existing positions are checked first.
    check_exits(ticker, candle, strategy_df)

    # Never carry positions overnight and never enter on the final candle.
    if candle["time"].hour == 15 and candle["time"].minute == 55:
        end_of_day_exit(ticker, candle)
        update_watch_state(ticker, strategy_df)
        return

    # Create a new signal only from a newly completed candle.
    create_pending_signal(ticker, strategy_df)
    update_watch_state(ticker, strategy_df)


# ============================================
# STARTUP
# ============================================

async def main():
    raw_data = load_today_history()
    five_df = prepare_strategy_data(raw_data)

    for ticker in TICKERS:
        ticker_df = five_df[five_df["ticker"] == ticker].copy()

        for _, row in ticker_df.iterrows():
            five_minute_history[ticker].append({
                "ticker": ticker,
                "time": row["time"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            })

    update_portfolio_value()

    print("Strategy history seeded.")
    print("Starting Telegram listener...")

    telegram_task = asyncio.create_task(
        telegram_poll_loop(state)
    )

    stream = StockDataStream(
        API_KEY,
        API_SECRET,
        feed=DataFeed.IEX,
    )

    for ticker in TICKERS:
        stream.subscribe_bars(handle_bar, ticker)

    print("========================================")
    print("LIVE 5-MINUTE IEX STREAM STARTED")
    print("========================================")

    try:
        await stream._run_forever()
    finally:
        state["online"] = False
        telegram_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
