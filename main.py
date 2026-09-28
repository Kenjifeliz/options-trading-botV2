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


# ============================================
# SETTINGS
# ============================================

TICKERS = [
    "AAPL",
    "AMD",
    "PLTR",
    "TSLA",
    "NVDA",
    "QQQ",
    "GOOG",
    "SPY",
    "META",
    "ORCL",
    "HIMS",
    "JPM",
    "NKE",
    "ASTS",
]

TIMEZONE = ZoneInfo("America/New_York")

START_TIME = pd.Timestamp("10:00").time()

CONTRACTS = 5

ORDERS_ENABLED = False

EMA_LENGTH = 9


# ============================================
# ALPACA CONNECTION
# ============================================

API_KEY = os.environ["APCA_API_KEY_ID"]
API_SECRET = os.environ["APCA_API_SECRET_KEY"]

PAPER_MODE = os.environ.get("ALPACA_PAPER", "true").lower() == "true"

print("========================================")
print("OPTIONS TRADING BOT")
print("STEP 11 - HISTORY + LIVE ENGINE")
print("========================================")
print(f"Paper mode: {'ENABLED' if PAPER_MODE else 'DISABLED'}")
print("Timeframe:   5 minutes")
print("Start time:  10:00 NY")
print("Data feed:   IEX")
print(f"Contracts:   {CONTRACTS}")
print(f"Orders:      {'ENABLED' if ORDERS_ENABLED else 'DISABLED'}")
print("========================================")


# ============================================
# HISTORICAL DATA CLIENT
# ============================================

historical_client = StockHistoricalDataClient(
    API_KEY,
    API_SECRET
)


# ============================================
# GET TODAY'S 1-MINUTE HISTORY
# ============================================

def load_today_history():

    now_ny = datetime.now(TIMEZONE)

    market_open = now_ny.replace(
        hour=9,
        minute=30,
        second=0,
        microsecond=0
    )

    # Only request completed data.
    # Leave the currently-forming minute out.
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

    # Alpaca returns symbol as a column when multiple tickers are requested.
    if "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df["timestamp"] = df["timestamp"].dt.tz_convert(
        "America/New_York"
    )

    df = df.rename(
        columns={
            "timestamp": "time",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )

    df = df[
        [
            "ticker",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ].copy()

    df = df.sort_values(
        ["ticker", "time"]
    ).reset_index(drop=True)

    print()
    print("Historical data loaded.")
    print(f"Rows:    {len(df):,}")
    print(f"Tickers: {df['ticker'].nunique()}")

    for ticker in TICKERS:
        count = len(df[df["ticker"] == ticker])
        print(f"{ticker:5s}: {count:,}")

    return df


# ============================================
# BUILD 5-MINUTE STRATEGY DATA
# ============================================

def prepare_strategy_data(raw_data):

    results = []

    for ticker in TICKERS:

        ticker_df = raw_data[
            raw_data["ticker"] == ticker
        ].copy()

        if ticker_df.empty:
            continue

        ticker_df = ticker_df.sort_values("time")

        ticker_df = ticker_df.set_index("time")

        five = ticker_df.resample(
            "5min",
            origin="start_day"
        ).agg(
            {
                "ticker": "first",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )

        five = five.dropna(
            subset=["open", "high", "low", "close"]
        )

        # EMA9 using OPEN, exactly as the strategy specifies.
        five["ema9"] = (
            five["open"]
            .ewm(
                span=EMA_LENGTH,
                adjust=False
            )
            .mean()
        )

        # Session VWAP using OPEN.
        five["date"] = five.index.date

        five["open_volume"] = (
            five["open"] * five["volume"]
        )

        five["cumulative_open_volume"] = (
            five.groupby("date")["open_volume"]
            .cumsum()
        )

        five["cumulative_volume"] = (
            five.groupby("date")["volume"]
            .cumsum()
        )

        five["vwap"] = (
            five["cumulative_open_volume"]
            / five["cumulative_volume"]
        )

        five = five.reset_index()

        results.append(five)

    if not results:
        raise RuntimeError(
            "No 5-minute strategy data could be created."
        )

    result = pd.concat(
        results,
        ignore_index=True
    )

    result = result.sort_values(
        ["ticker", "time"]
    ).reset_index(drop=True)

    return result


# ============================================
# SIGNAL CHECK
# ============================================

def check_completed_candle(ticker, df):

    if len(df) < 3:
        return

    previous = df.iloc[-2]
    signal = df.iloc[-1]

    signal_time = signal["time"]

    # Ignore anything before 10:00.
    if signal_time.time() < START_TIME:
        return

    # Must be same trading day.
    if previous["time"].date() != signal_time.date():
        return

    long_signal = (
        previous["close"] <= previous["vwap"]
        and signal["close"] > signal["vwap"]
    )

    short_signal = (
        previous["close"] >= previous["vwap"]
        and signal["close"] < signal["vwap"]
    )

    if not long_signal and not short_signal:
        return

    print()
    print("========================================")
    print("VWAP SIGNAL")
    print("========================================")
    print(f"Ticker:       {ticker}")
    print(f"Signal time:  {signal_time}")
    print(f"Close:        {signal['close']:.4f}")
    print(f"VWAP:         {signal['vwap']:.4f}")
    print(f"EMA9:         {signal['ema9']:.4f}")

    if long_signal:

        direction = "LONG"

        print("Direction:    LONG")

    else:

        direction = "SHORT"

        print("Direction:    SHORT")

    print("Waiting for NEXT 5-minute candle.")
    print("========================================")


# ============================================
# LIVE DATA
# ============================================

stream = StockDataStream(
    API_KEY,
    API_SECRET,
    feed=DataFeed.IEX
)


one_minute_bars = {
    ticker: []
    for ticker in TICKERS
}


five_minute_history = {
    ticker: []
    for ticker in TICKERS
}


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

    timestamp = timestamp.tz_convert(
        "America/New_York"
    )

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

    # Keep only recent live 1-minute bars.
    one_minute_bars[ticker] = (
        one_minute_bars[ticker][-5:]
    )

    # Only process completed 5-minute candles.
    #
    # Example:
    # 10:00, 10:01, 10:02, 10:03, 10:04
    # creates the completed 10:00 5-minute candle.
    if timestamp.minute % 5 != 4:
        return

    bars = one_minute_bars[ticker]

    if len(bars) < 5:
        return

    df = pd.DataFrame(bars)

    # Make sure all five bars belong to the same 5-minute bucket.
    bucket = timestamp.floor("5min")

    if not all(
        pd.Timestamp(x).floor("5min") == bucket
        for x in df["time"]
    ):
        return

    candle = {
        "ticker": ticker,
        "time": bucket,
        "open": df.iloc[0]["open"],
        "high": df["high"].max(),
        "low": df["low"].min(),
        "close": df.iloc[-1]["close"],
        "volume": df["volume"].sum(),
    }

    five_minute_history[ticker].append(candle)

    # Keep enough candles for EMA/VWAP calculations.
    five_minute_history[ticker] = (
        five_minute_history[ticker][-1000:]
    )

    strategy_df = pd.DataFrame(
        five_minute_history[ticker]
    )

    # Recalculate indicators.
    strategy_df["ema9"] = (
        strategy_df["open"]
        .ewm(
            span=EMA_LENGTH,
            adjust=False
        )
        .mean()
    )

    strategy_df["date"] = (
        strategy_df["time"].dt.date
    )

    strategy_df["open_volume"] = (
        strategy_df["open"]
        * strategy_df["volume"]
    )

    strategy_df["cumulative_open_volume"] = (
        strategy_df
        .groupby("date")["open_volume"]
        .cumsum()
    )

    strategy_df["cumulative_volume"] = (
        strategy_df
        .groupby("date")["volume"]
        .cumsum()
    )

    strategy_df["vwap"] = (
        strategy_df["cumulative_open_volume"]
        / strategy_df["cumulative_volume"]
    )

    check_completed_candle(
        ticker,
        strategy_df
    )


# ============================================
# MAIN
# ============================================

async def main():

    # ----------------------------------------
    # 1. Load today's completed history
    # ----------------------------------------

    raw_data = load_today_history()

    # ----------------------------------------
    # 2. Build 5-minute candles
    # ----------------------------------------

    five_df = prepare_strategy_data(
        raw_data
    )

    print()
    print("5-minute strategy data ready.")
    print(f"Rows: {len(five_df):,}")

    # ----------------------------------------
    # 3. Seed live strategy history
    # ----------------------------------------

    for ticker in TICKERS:

        ticker_df = five_df[
            five_df["ticker"] == ticker
        ].copy()

        if ticker_df.empty:
            continue

        for _, row in ticker_df.iterrows():

            five_minute_history[ticker].append(
                {
                    "ticker": ticker,
                    "time": row["time"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                }
            )

    print()
    print("Strategy history seeded.")

    for ticker in TICKERS:
        print(
            f"{ticker:5s}: "
            f"{len(five_minute_history[ticker])} "
            f"5-minute candles"
        )

    # ----------------------------------------
    # 4. Subscribe to live IEX bars
    # ----------------------------------------

    print()
    print("========================================")
    print("STARTING LIVE IEX STREAM")
    print("========================================")

    for ticker in TICKERS:
        stream.subscribe_bars(
            handle_bar,
            ticker
        )

    await stream._run_forever()


# ============================================
# START
# ============================================

if __name__ == "__main__":
    asyncio.run(main())
