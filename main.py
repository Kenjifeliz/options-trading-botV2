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
    "ASTS",
    "BE",
    "GOOG",
    "HIMS",
    "INTC",
    "IREN",
    "JPM",
    "MRVL",
    "NBIS",
    "NVDA",
    "ORCL",
    "PLTR",
    "QQQ",
    "RKLB",
    "SPCX",
    "SPY",
    "TSLA",
]

TIMEZONE = ZoneInfo("America/New_York")

TIMEFRAME = "5min"

START_TIME = pd.Timestamp("10:00").time()

CONTRACTS = 5

# Keep OFF while verifying the live signals.
ORDERS_ENABLED = False

EMA_LENGTH = 9

# Minimum distance between signal candle close and VWAP.
# Example:
# VWAP = 100.00
# LONG requires close >= 100.28
# SHORT requires close <= 99.72
VWAP_DISTANCE_THRESHOLD = 0.28


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
print("Timeframe:       5 minutes")
print("Start time:      10:00 NY")
print("Data feed:       IEX")
print(f"Contracts:       {CONTRACTS}")
print(f"VWAP filter:     {VWAP_DISTANCE_THRESHOLD:.2f}%")
print("EMA source:      OPEN")
print("VWAP source:     OPEN")
print("Entry:           NEXT 5-MINUTE OPEN")
print("Overlapping:     ENABLED")
print("Overnight:       DISABLED")
print(
    f"Orders:          "
    f"{'ENABLED' if ORDERS_ENABLED else 'DISABLED'}"
)
print(
    f"Paper mode:      "
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
# OPEN POSITIONS / ENTRY TRACKING
#
# Used only for EMA9 exit timing.
# Multiple overlapping positions are allowed.
# ============================================

active_entries = {
    ticker: []
    for ticker in TICKERS
}


# ============================================
# PENDING SIGNALS
# ============================================

pending_signals = {
    ticker: None
    for ticker in TICKERS
}


# ============================================
# SIGNAL CHECK
#
# A completed 5-minute candle creates the signal.
# Entry is attempted at the OPEN of the NEXT
# 5-minute candle.
# ============================================

def get_signal(ticker, df):

    if len(df) < 3:
        return None

    previous = df.iloc[-2]
    signal = df.iloc[-1]

    previous_time = pd.Timestamp(previous["time"])
    signal_time = pd.Timestamp(signal["time"])

    # Must be same trading day.
    if previous_time.date() != signal_time.date():
        return None

    # Ignore everything before 10:00 NY.
    if signal_time.time() < START_TIME:
        return None

    # VWAP cross.
    long_cross = (
        previous["close"] <= previous["vwap"]
        and signal["close"] > signal["vwap"]
    )

    short_cross = (
        previous["close"] >= previous["vwap"]
        and signal["close"] < signal["vwap"]
    )

    if not long_cross and not short_cross:
        return None

    signal_close = float(signal["close"])
    signal_vwap = float(signal["vwap"])

    if signal_vwap == 0:
        return None

    # Minimum 0.28% distance from VWAP.
    vwap_distance_pct = (
        abs(signal_close - signal_vwap)
        / abs(signal_vwap)
        * 100
    )

    if vwap_distance_pct < VWAP_DISTANCE_THRESHOLD:
        return None

    direction = "LONG" if long_cross else "SHORT"

    # Signal candle must close on the correct side
    # of EMA9.
    signal_ema9 = float(signal["ema9"])

    if direction == "LONG":
        ema_valid = signal_close > signal_ema9
    else:
        ema_valid = signal_close < signal_ema9

    if not ema_valid:
        return None

    return {
        "direction": direction,
        "signal_time": signal_time,
        "signal_close": signal_close,
        "signal_vwap": signal_vwap,
        "signal_ema9": signal_ema9,
        "vwap_distance_pct": vwap_distance_pct,
    }


# ============================================
# EXECUTE NEXT-CANDLE ENTRY
# ============================================

def execute_pending_entry(ticker, signal_info, entry_candle):

    if signal_info is None:
        return

    signal_time = pd.Timestamp(signal_info["signal_time"])
    entry_time = pd.Timestamp(entry_candle["time"])

    # Must be the next trading candle, never another day.
    if entry_time.date() != signal_time.date():
        return

    if entry_time <= signal_time:
        return

    direction = signal_info["direction"]
    entry_price = float(entry_candle["open"])
    signal_ema9 = float(signal_info["signal_ema9"])

    # NEXT candle OPEN must also be on the correct
    # side of the signal candle's EMA9.
    if direction == "LONG":
        position_valid = entry_price > signal_ema9
    else:
        position_valid = entry_price < signal_ema9

    if not position_valid:
        print()
        print("========================================")
        print("ENTRY REJECTED")
        print("========================================")
        print(f"Ticker:       {ticker}")
        print(f"Direction:    {direction}")
        print(f"Signal time:  {signal_time}")
        print(f"Entry time:   {entry_time}")
        print(f"Entry open:   {entry_price:.4f}")
        print(f"Signal EMA9:  {signal_ema9:.4f}")
        print("Reason: next candle opened on wrong side of EMA9.")
        print("========================================")
        return

    print()
    print("========================================")
    print("5-MINUTE VWAP ENTRY")
    print("========================================")
    print(f"Ticker:          {ticker}")
    print(f"Direction:       {direction}")
    print(f"Signal time:     {signal_time}")
    print(f"Signal close:    {signal_info['signal_close']:.4f}")
    print(f"Signal VWAP:     {signal_info['signal_vwap']:.4f}")
    print(f"VWAP distance:   {signal_info['vwap_distance_pct']:.4f}%")
    print(f"Signal EMA9:     {signal_ema9:.4f}")
    print(f"Entry time:      {entry_time}")
    print(f"Entry price:     {entry_price:.4f}")
    print("ENTRY:           VALID")
    print("========================================")

    # Every valid entry is stored separately.
    # Overlapping positions and multiple entries
    # on the same ticker are allowed.
    active_entries[ticker].append(
        {
            "direction": direction,
            "entry_time": entry_time,
            "entry_price": entry_price,
            "entry_ema9": signal_ema9,
        }
    )

    if not ORDERS_ENABLED:
        print("PAPER ENTRY DISABLED")
        print("Valid signal recorded, but no option order was submitted.")
        print("========================================")
        return

    # Actual option order execution will be added
    # after signal verification.
    # DO NOT add an order here yet.


# ============================================
# EMA9 EXIT CHECK
#
# Exit only when a COMPLETED candle CLOSES
# through EMA9.
#
# Long:  close < EMA9
# Short: close > EMA9
#
# Wicks are ignored.
# ============================================

def check_exits(ticker, current_candle, strategy_df):

    if not active_entries[ticker]:
        return

    current_time = pd.Timestamp(current_candle["time"])
    current_close = float(current_candle["close"])
    current_ema9 = float(strategy_df.iloc[-1]["ema9"])

    remaining_entries = []

    for entry in active_entries[ticker]:

        entry_time = pd.Timestamp(entry["entry_time"])
        direction = entry["direction"]

        # Never carry positions overnight.
        if entry_time.date() != current_time.date():
            continue

        # Do not check the entry candle itself.
        if current_time <= entry_time:
            remaining_entries.append(entry)
            continue

        if direction == "LONG":
            should_exit = current_close < current_ema9
        else:
            should_exit = current_close > current_ema9

        if should_exit:

            print()
            print("========================================")
            print("EMA9 EXIT SIGNAL")
            print("========================================")
            print(f"Ticker:       {ticker}")
            print(f"Direction:    {direction}")
            print(f"Entry time:   {entry_time}")
            print(f"Entry price:  {entry['entry_price']:.4f}")
            print(f"Exit time:    {current_time}")
            print(f"Exit price:   {current_close:.4f}")
            print(f"EMA9:         {current_ema9:.4f}")
            print("Exit reason:  EMA9")
            print("========================================")

            # Actual option exit order will be added
            # after signal verification.

        else:
            remaining_entries.append(entry)

    active_entries[ticker] = remaining_entries


# ============================================
# END-OF-DAY EXIT
#
# Regular session final candle is normally 15:55.
# ============================================

def check_end_of_day(ticker, current_candle):

    current_time = pd.Timestamp(current_candle["time"])

    if current_time.hour != 15 or current_time.minute != 55:
        return

    if not active_entries[ticker]:
        return

    current_close = float(current_candle["close"])

    print()
    print("========================================")
    print("END OF DAY EXIT")
    print("========================================")
    print(f"Ticker:       {ticker}")
    print(f"Exit time:    {current_time}")
    print(f"Exit price:   {current_close:.4f}")
    print(f"Positions:    {len(active_entries[ticker])}")
    print("Exit reason:  EOD")
    print("========================================")

    # Actual option exit orders will be added
    # after signal verification.

    active_entries[ticker] = []


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

    timestamp = pd.Timestamp(bar.timestamp)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    timestamp = timestamp.tz_convert("America/New_York")

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

    # Keep the five 1-minute bars needed to form
    # the current completed 5-minute candle.
    one_minute_bars[ticker] = (
        one_minute_bars[ticker][-5:]
    )

    # Only process the completed 5-minute candle
    # after minute 04, 09, 14, etc.
    if timestamp.minute % 5 != 4:
        return

    bars = one_minute_bars[ticker]

    if len(bars) < 5:
        return

    df = pd.DataFrame(bars)

    bucket = timestamp.floor("5min")

    if not all(
        pd.Timestamp(x).floor("5min") == bucket
        for x in df["time"]
    ):
        return

    candle = {
        "ticker": ticker,
        "time": bucket,
        "open": float(df.iloc[0]["open"]),
        "high": float(df["high"].max()),
        "low": float(df["low"].min()),
        "close": float(df.iloc[-1]["close"]),
        "volume": float(df["volume"].sum()),
    }

    five_minute_history[ticker].append(candle)

    five_minute_history[ticker] = (
        five_minute_history[ticker][-1000:]
    )

    strategy_df = pd.DataFrame(
        five_minute_history[ticker]
    )

    if len(strategy_df) < 3:
        return

    # ========================================
    # EMA9 — SOURCE = OPEN
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
    # SESSION VWAP — SOURCE = OPEN
    # ========================================

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

    # ========================================
    # 1. EXECUTE PREVIOUS CANDLE'S SIGNAL
    #    AT THIS CANDLE'S OPEN
    # ========================================

    if pending_signals[ticker] is not None:

        execute_pending_entry(
            ticker,
            pending_signals[ticker],
            candle
        )

        pending_signals[ticker] = None

    # ========================================
    # 2. EXISTING POSITION EXITS
    # ========================================

    check_exits(
        ticker,
        candle,
        strategy_df
    )

    # ========================================
    # 3. END OF DAY
    # ========================================

    check_end_of_day(
        ticker,
        candle
    )

    # ========================================
    # 4. CREATE NEW SIGNAL
    #
    # This signal cannot enter until the NEXT
    # completed 5-minute candle, whose OPEN is
    # used as the entry price.
    # ========================================

    signal_info = get_signal(
        ticker,
        strategy_df
    )

    if signal_info is not None:

        pending_signals[ticker] = signal_info

        print()
        print("========================================")
        print("QUALIFYING VWAP SIGNAL")
        print("========================================")
        print(f"Ticker:        {ticker}")
        print(f"Direction:     {signal_info['direction']}")
        print(f"Signal time:   {signal_info['signal_time']}")
        print(f"Signal close:  {signal_info['signal_close']:.4f}")
        print(f"Signal VWAP:   {signal_info['signal_vwap']:.4f}")
        print(f"VWAP distance: {signal_info['vwap_distance_pct']:.4f}%")
        print(f"Signal EMA9:   {signal_info['signal_ema9']:.4f}")
        print("Entry:         NEXT 5-MINUTE CANDLE OPEN")
        print("========================================")



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
