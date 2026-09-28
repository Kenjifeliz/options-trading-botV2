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

TIMEFRAME = "5min"

START_TIME = pd.Timestamp("10:00").time()

CONTRACTS = 5

# KEEP OFF while we verify the 5-minute signals.
ORDERS_ENABLED = False

EMA_LENGTH = 9


# ============================================
# ALPACA CONNECTION
# ============================================

API_KEY = os.environ["APCA_API_KEY_ID"]
API_SECRET = os.environ["APCA_API_SECRET_KEY"]

PAPER_MODE = (
    os.environ.get("ALPACA_PAPER", "true").lower() == "true"
)


print("========================================")
print("OPTIONS TRADING BOT")
print("========================================")
print("Timeframe:   5 minutes")
print("Start time:  10:00 NY")
print("Data feed:   IEX")
print(f"Contracts:   {CONTRACTS}")
print(
    f"Orders:      "
    f"{'ENABLED' if ORDERS_ENABLED else 'DISABLED'}"
)
print(
    f"Paper mode:  "
    f"{'ENABLED' if PAPER_MODE else 'DISABLED'}"
)
print("========================================")


# ============================================
# HISTORICAL CLIENT
# ============================================

historical_client = StockHistoricalDataClient(
    API_KEY,
    API_SECRET
)


# ============================================
# TODAY'S 1-MINUTE HISTORY
# ============================================

def load_today_history():

    now_ny = datetime.now(TIMEZONE)

    market_open = now_ny.replace(
        hour=9,
        minute=30,
        second=0,
        microsecond=0
    )

    # Do not include the currently forming minute.
    end_time = now_ny - timedelta(minutes=1)

    print()
    print("Loading today's historical IEX bars...")
    print(f"Start: {market_open}")
    print(f"End:   {end_time}")

    request = StockBarsRequest(
        symbol_or_symbols=TICKERS,
        timeframe=TimeFrame(
            1,
            TimeFrameUnit.Minute
        ),
        start=market_open,
        end=end_time,
        feed=DataFeed.IEX,
        limit=10000,
    )

    bars = historical_client.get_stock_bars(
        request
    )

    df = bars.df.reset_index()

    if df.empty:
        raise RuntimeError(
            "No historical bars were returned."
        )

    if "symbol" in df.columns:
        df = df.rename(
            columns={"symbol": "ticker"}
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        utc=True
    )

    df["timestamp"] = (
        df["timestamp"]
        .dt.tz_convert("America/New_York")
    )

    df = df.rename(
        columns={
            "timestamp": "time"
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
        count = len(
            df[df["ticker"] == ticker]
        )
        print(
            f"{ticker:5s}: {count:,}"
        )

    return df


# ============================================
# BUILD 5-MINUTE DATA
# ============================================

def prepare_strategy_data(raw_data):

    results = []

    for ticker in TICKERS:

        ticker_df = raw_data[
            raw_data["ticker"] == ticker
        ].copy()

        if ticker_df.empty:
            continue

        ticker_df = ticker_df.sort_values(
            "time"
        )

        ticker_df = ticker_df.set_index(
            "time"
        )

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
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        )

        # ====================================
        # EMA9
        # Source = OPEN
        # ====================================

        five["ema9"] = (
            five["open"]
            .ewm(
                span=EMA_LENGTH,
                adjust=False
            )
            .mean()
        )

        # ====================================
        # SESSION VWAP
        # Source = OPEN
        # ====================================

        five["date"] = five.index.date

        five["open_volume"] = (
            five["open"]
            * five["volume"]
        )

        five["cumulative_open_volume"] = (
            five
            .groupby("date")["open_volume"]
            .cumsum()
        )

        five["cumulative_volume"] = (
            five
            .groupby("date")["volume"]
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
            "No 5-minute strategy data created."
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
# LIVE STRATEGY HISTORY
# ============================================

five_minute_history = {
    ticker: []
    for ticker in TICKERS
}


# ============================================
# PENDING VWAP SIGNALS
# ============================================

pending_signals = {}


# ============================================
# SIGNAL CHECK
# ============================================

def check_signal(ticker, df):

    if len(df) < 3:
        return

    previous = df.iloc[-2]
    signal = df.iloc[-1]

    previous_time = pd.Timestamp(
        previous["time"]
    )

    signal_time = pd.Timestamp(
        signal["time"]
    )

    # Must be same trading day.
    if previous_time.date() != signal_time.date():
        return

    # Ignore before 10:00.
    if signal_time.time() < START_TIME:
        return

    # ========================================
    # VWAP CROSS
    # ========================================

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

    direction = (
        "LONG"
        if long_signal
        else "SHORT"
    )

    print()
    print("========================================")
    print("5-MINUTE VWAP SIGNAL")
    print("========================================")
    print(f"Ticker:          {ticker}")
    print(f"Signal time:     {signal_time}")
    print(f"Previous close:  {previous['close']:.4f}")
    print(f"Previous VWAP:   {previous['vwap']:.4f}")
    print(f"Signal close:    {signal['close']:.4f}")
    print(f"Signal VWAP:     {signal['vwap']:.4f}")
    print(f"Signal EMA9:     {signal['ema9']:.4f}")
    print(f"Direction:       {direction}")
    print("Waiting for NEXT 5-minute candle.")
    print("========================================")

    # Store the signal.
    pending_signals[ticker] = {
        "direction": direction,
        "signal_time": signal_time,
        "signal_ema9": float(
            signal["ema9"]
        ),
    }


# ============================================
# NEXT-CANDLE ENTRY TEST
# ============================================

def process_pending_entry(
    ticker,
    current_candle
):

    if ticker not in pending_signals:
        return

    pending = pending_signals.pop(
        ticker
    )

    direction = pending["direction"]

    signal_time = pending[
        "signal_time"
    ]

    signal_ema9 = pending[
        "signal_ema9"
    ]

    entry_time = pd.Timestamp(
        current_candle["time"]
    )

    entry_open = float(
        current_candle["open"]
    )

    # Never carry a signal overnight.
    if entry_time.date() != signal_time.date():
        print(
            f"{ticker}: pending signal "
            f"discarded — next candle is "
            f"next trading day."
        )
        return

    # Entry itself must be after 10:00.
    if entry_time.time() < START_TIME:
        return

    print()
    print("========================================")
    print("NEXT-CANDLE ENTRY TEST")
    print("========================================")
    print(f"Ticker:          {ticker}")
    print(f"Signal time:     {signal_time}")
    print(f"Entry time:      {entry_time}")
    print(f"Direction:       {direction}")
    print(f"Signal EMA9:     {signal_ema9:.4f}")
    print(f"Next candle OPEN:{entry_open:.4f}")

    if direction == "LONG":

        valid = (
            entry_open > signal_ema9
        )

        print(
            f"Test: {entry_open:.4f} "
            f"> {signal_ema9:.4f}"
        )

    else:

        valid = (
            entry_open < signal_ema9
        )

        print(
            f"Test: {entry_open:.4f} "
            f"< {signal_ema9:.4f}"
        )

    if not valid:

        print("ENTRY: REJECTED")
        print(
            "Reason: EMA9 entry filter failed."
        )
        print("========================================")

        return

    print("ENTRY: VALID")

    if not ORDERS_ENABLED:

        print()
        print("PAPER ENTRY DISABLED")
        print(
            "Signal is valid, but no order "
            "was submitted."
        )
        print("========================================")

        return

    # Orders intentionally disabled in this version.
    # We are first verifying the 5-minute
    # signals against TradingView.


# ============================================
# LIVE STREAM
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


# ============================================
# LIVE BAR HANDLER
# ============================================

async def handle_bar(bar):

    ticker = bar.symbol

    if ticker not in one_minute_bars:
        return

    timestamp = pd.Timestamp(
        bar.timestamp
    )

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(
            "UTC"
        )

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

    one_minute_bars[ticker].append(
        record
    )

    # Keep recent 1-minute bars.
    one_minute_bars[ticker] = (
        one_minute_bars[ticker][-5:]
    )

    # ========================================
    # ONLY PROCESS COMPLETED 5-MINUTE BAR
    # ========================================

    if timestamp.minute % 5 != 4:
        return

    bars = one_minute_bars[ticker]

    if len(bars) < 5:
        return

    df = pd.DataFrame(bars)

    bucket = timestamp.floor(
        "5min"
    )

    if not all(
        pd.Timestamp(x).floor("5min")
        == bucket
        for x in df["time"]
    ):
        return

    candle = {
        "ticker": ticker,
        "time": bucket,
        "open": float(
            df.iloc[0]["open"]
        ),
        "high": float(
            df["high"].max()
        ),
        "low": float(
            df["low"].min()
        ),
        "close": float(
            df.iloc[-1]["close"]
        ),
        "volume": float(
            df["volume"].sum()
        ),
    }

    # ========================================
    # CHECK WHETHER THIS IS THE NEXT CANDLE
    # FOR A PREVIOUS SIGNAL
    # ========================================

    process_pending_entry(
        ticker,
        candle
    )

    # ========================================
    # ADD COMPLETED 5-MIN CANDLE
    # ========================================

    five_minute_history[
        ticker
    ].append(candle)

    five_minute_history[
        ticker
    ] = five_minute_history[
        ticker
    ][-1000:]

    strategy_df = pd.DataFrame(
        five_minute_history[ticker]
    )

    if len(strategy_df) < 3:
        return

    # ========================================
    # EMA9
    # ========================================

    strategy_df["ema9"] = (
        strategy_df["open"]
        .ewm(
            span=EMA_LENGTH,
            adjust=False
        )
        .mean()
    )

    # ========================================
    # SESSION VWAP
    # ========================================

    strategy_df["date"] = (
        strategy_df["time"].dt.date
    )

    strategy_df["open_volume"] = (
        strategy_df["open"]
        * strategy_df["volume"]
    )

    strategy_df[
        "cumulative_open_volume"
    ] = (
        strategy_df
        .groupby("date")["open_volume"]
        .cumsum()
    )

    strategy_df[
        "cumulative_volume"
    ] = (
        strategy_df
        .groupby("date")["volume"]
        .cumsum()
    )

    strategy_df["vwap"] = (
        strategy_df[
            "cumulative_open_volume"
        ]
        / strategy_df[
            "cumulative_volume"
        ]
    )

    # ========================================
    # CHECK NEW VWAP SIGNAL
    # ========================================

    check_signal(
        ticker,
        strategy_df
    )


# ============================================
# MAIN
# ============================================

async def main():

    # ----------------------------------------
    # 1. Load today's history
    # ----------------------------------------

    raw_data = load_today_history()

    # ----------------------------------------
    # 2. Convert to 5-minute candles
    # ----------------------------------------

    five_df = prepare_strategy_data(
        raw_data
    )

    print()
    print("5-minute strategy data ready.")
    print(
        f"Rows: {len(five_df):,}"
    )

    # ----------------------------------------
    # 3. Seed live history
    # ----------------------------------------

    for ticker in TICKERS:

        ticker_df = five_df[
            five_df["ticker"] == ticker
        ].copy()

        if ticker_df.empty:
            continue

        for _, row in ticker_df.iterrows():

            five_minute_history[
                ticker
            ].append(
                {
                    "ticker": ticker,
                    "time": row["time"],
                    "open": float(
                        row["open"]
                    ),
                    "high": float(
                        row["high"]
                    ),
                    "low": float(
                        row["low"]
                    ),
                    "close": float(
                        row["close"]
                    ),
                    "volume": float(
                        row["volume"]
                    ),
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
    # 4. Start live stream
    # ----------------------------------------

    print()
    print("========================================")
    print("STARTING LIVE 5-MINUTE IEX STREAM")
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
