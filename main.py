import os
import asyncio
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca.data.live import StockDataStream


# ============================================================
# SETTINGS
# ============================================================

TICKERS = [
    "AAPL", "AMD", "PLTR", "TSLA", "NVDA",
    "QQQ", "GOOG", "SPY", "META", "ORCL",
    "HIMS", "JPM", "NKE", "ASTS"
]

TIMEFRAME_MINUTES = 5
START_TIME = dt_time(10, 0)

EMA_LENGTH = 9

CONTRACTS = 5
OPTION_MULTIPLIER = 100

NY_TZ = ZoneInfo("America/New_York")


# ============================================================
# DATA STORAGE
# ============================================================

one_minute_bars = {
    ticker: []
    for ticker in TICKERS
}

completed_candles = {
    ticker: []
    for ticker in TICKERS
}


# ============================================================
# INDICATOR CALCULATIONS
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    # EMA9 using OPEN
    df["ema9"] = (
        df["open"]
        .ewm(
            span=EMA_LENGTH,
            adjust=False
        )
        .mean()
    )

    # Session VWAP using OPEN
    df["date"] = df["timestamp"].dt.date

    df["open_volume"] = (
        df["open"] * df["volume"]
    )

    df["cumulative_open_volume"] = (
        df.groupby("date")["open_volume"]
        .cumsum()
    )

    df["cumulative_volume"] = (
        df.groupby("date")["volume"]
        .cumsum()
    )

    df["vwap"] = (
        df["cumulative_open_volume"]
        /
        df["cumulative_volume"]
    )

    return df


# ============================================================
# VWAP SIGNAL
# ============================================================

def check_signal(df):

    if len(df) < 2:
        return None

    previous = df.iloc[-2]
    current = df.iloc[-1]

    current_time = current["timestamp"].time()

    # Ignore everything before 10:00 NY
    if current_time < START_TIME:
        return None

    # LONG:
    # previous close <= previous VWAP
    # current close > current VWAP

    long_signal = (
        previous["close"] <= previous["vwap"]
        and
        current["close"] > current["vwap"]
    )

    # SHORT:
    # previous close >= previous VWAP
    # current close < current VWAP

    short_signal = (
        previous["close"] >= previous["vwap"]
        and
        current["close"] < current["vwap"]
    )

    if not long_signal and not short_signal:
        return None

    direction = (
        "LONG"
        if long_signal
        else
        "SHORT"
    )

    return {
        "direction": direction,
        "signal_time": current["timestamp"],
        "signal_close": float(current["close"]),
        "signal_vwap": float(current["vwap"]),
        "signal_ema9": float(current["ema9"])
    }


# ============================================================
# PROCESS COMPLETED 5-MINUTE CANDLE
# ============================================================

def process_candle(ticker, candle):

    completed_candles[ticker].append(candle)

    # Keep enough history for EMA9/VWAP
    if len(completed_candles[ticker]) > 500:
        completed_candles[ticker] = (
            completed_candles[ticker][-500:]
        )

    df = pd.DataFrame(
        completed_candles[ticker]
    )

    df = calculate_indicators(df)

    # Need at least two completed candles
    if len(df) < 2:
        return

    signal = check_signal(df)

    if signal is None:
        return

    # --------------------------------------------------------
    # NEXT CANDLE ENTRY PRICE
    #
    # We do NOT have the next candle open yet.
    # Therefore we record the signal and wait.
    # --------------------------------------------------------

    print("")
    print("================================================")
    print("VWAP SIGNAL DETECTED")
    print("================================================")
    print(f"Ticker:        {ticker}")
    print(f"Direction:     {signal['direction']}")
    print(f"Signal time:   {signal['signal_time']}")
    print(f"Signal close:  ${signal['signal_close']:.2f}")
    print(f"VWAP:          ${signal['signal_vwap']:.2f}")
    print(f"EMA9:          ${signal['signal_ema9']:.2f}")
    print("Contracts:     5")
    print("ORDER:         DISABLED")
    print("================================================")
    print("")


# ============================================================
# ALPACA LIVE BAR HANDLER
# ============================================================

async def on_bar(bar):

    ticker = bar.symbol

    if ticker not in TICKERS:
        return

    timestamp = bar.timestamp.astimezone(NY_TZ)

    # --------------------------------------------------------
    # Convert incoming 1-minute bar
    # --------------------------------------------------------

    minute_bar = {
        "timestamp": timestamp,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume)
    }

    one_minute_bars[ticker].append(
        minute_bar
    )

    # --------------------------------------------------------
    # Keep recent history
    # --------------------------------------------------------

    if len(one_minute_bars[ticker]) > 500:
        one_minute_bars[ticker] = (
            one_minute_bars[ticker][-500:]
        )

    # --------------------------------------------------------
    # Only process a completed 5-minute block
    #
    # Example:
    # 10:00, 10:01, 10:02, 10:03, 10:04
    # becomes the 10:00 5-minute candle.
    # --------------------------------------------------------

    minute = timestamp.minute

    if minute % TIMEFRAME_MINUTES != (
        TIMEFRAME_MINUTES - 1
    ):
        return

    recent = one_minute_bars[ticker][-5:]

    if len(recent) < 5:
        return

    timestamps = [
        x["timestamp"]
        for x in recent
    ]

    # Make sure these are five consecutive minutes
    expected_minutes = pd.date_range(
        start=timestamps[0],
        periods=5,
        freq="min",
        tz=NY_TZ
    )

    actual_minutes = pd.DatetimeIndex(
        timestamps
    )

    if not actual_minutes.equals(
        expected_minutes
    ):
        return

    # --------------------------------------------------------
    # Build 5-minute candle
    # --------------------------------------------------------

    candle = {
        "timestamp": recent[0]["timestamp"],
        "open": recent[0]["open"],
        "high": max(
            x["high"] for x in recent
        ),
        "low": min(
            x["low"] for x in recent
        ),
        "close": recent[-1]["close"],
        "volume": sum(
            x["volume"] for x in recent
        )
    }

    process_candle(
        ticker,
        candle
    )


# ============================================================
# MAIN
# ============================================================

async def run_stream():

    api_key = os.environ.get(
        "APCA_API_KEY_ID"
    )

    api_secret = os.environ.get(
        "APCA_API_SECRET_KEY"
    )

    if not api_key:
        raise RuntimeError(
            "Missing APCA_API_KEY_ID"
        )

    if not api_secret:
        raise RuntimeError(
            "Missing APCA_API_SECRET_KEY"
        )

    # SAFETY LOCK
    if os.environ.get(
        "ALPACA_PAPER",
        "true"
    ).lower() != "true":

        raise RuntimeError(
            "SAFETY ERROR: ALPACA_PAPER must be true."
        )

    print("========================================")
    print("OPTIONS TRADING BOT")
    print("STEP 10 - LIVE STRATEGY ENGINE")
    print("========================================")
    print("Paper mode: ENABLED")
    print("Timeframe:   5 minutes")
    print("Start time:  10:00 NY")
    print("Contracts:   5")
    print("Orders:      DISABLED")
    print("========================================")
    print("")

    stream = StockDataStream(
        api_key,
        api_secret
    )

    stream.subscribe_bars(
        on_bar,
        *TICKERS
    )

    print(
        "Subscribed to live bars for "
        f"{len(TICKERS)} tickers."
    )

    print(
        "Waiting for completed 5-minute candles..."
    )

    await stream._run_forever()


def main():

    asyncio.run(
        run_stream()
    )


if __name__ == "__main__":
    main()
