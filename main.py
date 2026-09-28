import os
import asyncio
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo

import pandas as pd

from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


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
# STORAGE
# ============================================================

completed_candles = {
    ticker: []
    for ticker in TICKERS
}

one_minute_bars = {
    ticker: []
    for ticker in TICKERS
}


# ============================================================
# INDICATORS
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
# BUILD 5-MINUTE CANDLES FROM 1-MINUTE DATA
# ============================================================

def build_5min_candles(minute_df):

    df = minute_df.copy()

    df = df.sort_values("timestamp")

    df = df.set_index("timestamp")

    result = (
        df.resample("5min", origin="start_day")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        })
    )

    result = result.dropna()

    result = result.reset_index()

    return result


# ============================================================
# LOAD TODAY'S HISTORY
# ============================================================

def load_today_history(api_key, api_secret):

    print("")
    print("========================================")
    print("LOADING TODAY'S MARKET HISTORY")
    print("========================================")

    data_client = StockHistoricalDataClient(
        api_key,
        api_secret
    )

    now_ny = datetime.now(NY_TZ)

    today_start = datetime(
        now_ny.year,
        now_ny.month,
        now_ny.day,
        9,
        30,
        tzinfo=NY_TZ
    )

    # We deliberately stop before the current minute.
    # The live stream will provide the current/future bars.
    now_utc = datetime.now(
        ZoneInfo("UTC")
    )

    request = StockBarsRequest(
        symbol_or_symbols=TICKERS,
        timeframe=TimeFrame(
            1,
            TimeFrameUnit.Minute
        ),
        start=today_start,
        end=now_utc
    )

    bars = data_client.get_stock_bars(
        request
    ).df

    if bars.empty:
        raise RuntimeError(
            "No historical bars were returned."
        )

    print(
        f"Loaded {len(bars):,} one-minute bars."
    )

    # --------------------------------------------------------
    # Process each ticker separately
    # --------------------------------------------------------

    for ticker in TICKERS:

        if ticker not in bars.index.get_level_values(
            "symbol"
        ):
            print(
                f"{ticker}: NO DATA"
            )
            continue

        ticker_df = bars.xs(
            ticker,
            level="symbol"
        ).reset_index()

        ticker_df = ticker_df.rename(
            columns={
                "timestamp": "timestamp",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume"
            }
        )

        ticker_df["timestamp"] = (
            pd.to_datetime(
                ticker_df["timestamp"],
                utc=True
            ).dt.tz_convert(NY_TZ)
        )

        # ----------------------------------------------------
        # Keep regular market hours
        # ----------------------------------------------------

        ticker_df = ticker_df[
            (
                ticker_df["timestamp"].dt.time
                >= dt_time(9, 30)
            )
            &
            (
                ticker_df["timestamp"].dt.time
                <= dt_time(15, 59)
            )
        ].copy()

        # ----------------------------------------------------
        # Build completed 5-minute candles
        # ----------------------------------------------------

        five_df = build_5min_candles(
            ticker_df[
                [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            ]
        )

        # ----------------------------------------------------
        # Only use candles whose five-minute interval
        # has actually completed.
        # ----------------------------------------------------

        current_time = datetime.now(
            NY_TZ
        )

        five_df = five_df[
            five_df["timestamp"]
            + pd.Timedelta(minutes=5)
            <= current_time
        ].copy()

        if five_df.empty:
            continue

        five_df = calculate_indicators(
            five_df
        )

        records = five_df.to_dict(
            "records"
        )

        completed_candles[ticker] = records

        print(
            f"{ticker}: "
            f"{len(records)} completed 5m candles"
        )

    print("========================================")
    print("HISTORY LOAD COMPLETE")
    print("========================================")


# ============================================================
# SIGNAL CHECK
# ============================================================

def check_signal(ticker):

    candles = completed_candles[ticker]

    if len(candles) < 2:
        return None

    previous = candles[-2]
    current = candles[-1]

    current_time = current["timestamp"].time()

    if current_time < START_TIME:
        return None

    long_signal = (
        previous["close"] <= previous["vwap"]
        and
        current["close"] > current["vwap"]
    )

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

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # The EMA9 entry filter compares EMA9 from the completed
    # signal candle with the NEXT candle's OPEN.
    #
    # We don't know that next open yet.
    #
    # Therefore the signal is stored for evaluation when
    # the next 5-minute candle arrives.
    # --------------------------------------------------------

    return {
        "direction": direction,
        "signal_time": current["timestamp"],
        "signal_close": float(current["close"]),
        "signal_vwap": float(current["vwap"]),
        "signal_ema9": float(current["ema9"])
    }


# ============================================================
# PROCESS NEW COMPLETED 5-MINUTE CANDLE
# ============================================================

def process_new_candle(ticker, candle):

    completed_candles[ticker].append(
        candle
    )

    if len(completed_candles[ticker]) > 500:
        completed_candles[ticker] = (
            completed_candles[ticker][-500:]
        )

    # Recalculate indicators
    df = pd.DataFrame(
        completed_candles[ticker]
    )

    df = calculate_indicators(
        df
    )

    completed_candles[ticker] = (
        df.to_dict("records")
    )

    # --------------------------------------------------------
    # The newly completed candle can now be the ENTRY candle
    # for a signal generated on the previous candle.
    #
    # We therefore check the previous candle for a signal.
    # --------------------------------------------------------

    if len(df) < 3:
        return

    signal_candle = df.iloc[-2]
    entry_candle = df.iloc[-1]

    previous_candle = df.iloc[-3]

    signal_time = signal_candle["timestamp"]

    if signal_time.time() < START_TIME:
        return

    long_signal = (
        previous_candle["close"]
        <= previous_candle["vwap"]
        and
        signal_candle["close"]
        > signal_candle["vwap"]
    )

    short_signal = (
        previous_candle["close"]
        >= previous_candle["vwap"]
        and
        signal_candle["close"]
        < signal_candle["vwap"]
    )

    if not long_signal and not short_signal:
        return

    entry_price = float(
        entry_candle["open"]
    )

    ema9 = float(
        signal_candle["ema9"]
    )

    # --------------------------------------------------------
    # EMA9 ENTRY FILTER
    #
    # LONG:
    # entry price > EMA9
    #
    # SHORT:
    # entry price < EMA9
    # --------------------------------------------------------

    if long_signal:

        if not entry_price > ema9:
            print(
                f"{ticker}: LONG rejected by EMA9 filter"
            )
            return

        direction = "LONG"

    else:

        if not entry_price < ema9:
            print(
                f"{ticker}: SHORT rejected by EMA9 filter"
            )
            return

        direction = "SHORT"

    # --------------------------------------------------------
    # VALID SIGNAL
    # --------------------------------------------------------

    print("")
    print("================================================")
    print("VALID STRATEGY SIGNAL")
    print("================================================")
    print(f"Ticker:          {ticker}")
    print(f"Direction:       {direction}")
    print(f"Signal candle:   {signal_time}")
    print(f"Entry candle:    {entry_candle['timestamp']}")
    print(f"Entry price:     ${entry_price:.2f}")
    print(f"EMA9:            ${ema9:.2f}")
    print(f"VWAP:            ${float(signal_candle['vwap']):.2f}")
    print(f"Contracts:       {CONTRACTS}")
    print("ORDER:           DISABLED")
    print("================================================")
    print("")


# ============================================================
# LIVE BAR HANDLER
# ============================================================

async def on_bar(bar):

    ticker = bar.symbol

    if ticker not in TICKERS:
        return

    timestamp = (
        bar.timestamp
        .astimezone(NY_TZ)
    )

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

    if len(one_minute_bars[ticker]) > 10:
        one_minute_bars[ticker] = (
            one_minute_bars[ticker][-10:]
        )

    # --------------------------------------------------------
    # Only process at the end of each 5-minute block.
    # --------------------------------------------------------

    if timestamp.minute % 5 != 4:
        return

    recent = one_minute_bars[ticker][-5:]

    if len(recent) < 5:
        return

    timestamps = [
        x["timestamp"]
        for x in recent
    ]

    expected = pd.date_range(
        start=timestamps[0],
        periods=5,
        freq="min",
        tz=NY_TZ
    )

    actual = pd.DatetimeIndex(
        timestamps
    )

    if not actual.equals(expected):
        return

    candle = {
        "timestamp": recent[0]["timestamp"],
        "open": recent[0]["open"],
        "high": max(
            x["high"]
            for x in recent
        ),
        "low": min(
            x["low"]
            for x in recent
        ),
        "close": recent[-1]["close"],
        "volume": sum(
            x["volume"]
            for x in recent
        )
    }

    process_new_candle(
        ticker,
        candle
    )


# ============================================================
# MAIN STREAM
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

    if os.environ.get(
        "ALPACA_PAPER",
        "true"
    ).lower() != "true":

        raise RuntimeError(
            "SAFETY ERROR: ALPACA_PAPER must be true."
        )

    print("========================================")
    print("OPTIONS TRADING BOT")
    print("STEP 11 - HISTORY + LIVE ENGINE")
    print("========================================")
    print("Paper mode: ENABLED")
    print("Timeframe:   5 minutes")
    print("Start time:  10:00 NY")
    print("Contracts:   5")
    print("Orders:      DISABLED")
    print("========================================")

    # --------------------------------------------------------
    # Load history first
    # --------------------------------------------------------

    load_today_history(
        api_key,
        api_secret
    )

    # --------------------------------------------------------
    # Start live stream
    # --------------------------------------------------------

    stream = StockDataStream(
        api_key,
        api_secret
    )

    stream.subscribe_bars(
        on_bar,
        *TICKERS
    )

    print("")
    print("========================================")
    print("LIVE STREAM STARTED")
    print("========================================")
    print(
        f"Subscribed to {len(TICKERS)} tickers."
    )
    print("Waiting for completed 5-minute candles...")
    print("")

    await stream._run_forever()


def main():

    asyncio.run(
        run_stream()
    )


if __name__ == "__main__":
    main()
