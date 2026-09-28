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
from alpaca.trading.requests import (
    GetOptionContractsRequest,
    MarketOrderRequest,
    ClosePositionRequest,
)
from alpaca.trading.enums import (
    AssetStatus,
    ContractType,
    OrderSide,
    OrderType,
    TimeInForce,
)


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
EOD_TIME = pd.Timestamp("15:59").time()

CONTRACTS = 5

# SAFETY: PAPER ONLY
PAPER_MODE = True
ORDERS_ENABLED = True

EMA_LENGTH = 9


# ============================================
# ALPACA CONNECTION
# ============================================

API_KEY = os.environ["APCA_API_KEY_ID"]
API_SECRET = os.environ["APCA_API_SECRET_KEY"]

if not PAPER_MODE:
    raise RuntimeError(
        "SAFETY STOP: PAPER_MODE must remain True."
    )

historical_client = StockHistoricalDataClient(
    API_KEY,
    API_SECRET
)

trading_client = TradingClient(
    API_KEY,
    API_SECRET,
    paper=True
)


# ============================================
# STARTUP
# ============================================

print("========================================")
print("OPTIONS TRADING BOT")
print("STEP 13 - 1-MINUTE PAPER OPTIONS")
print("========================================")
print("Paper mode: ENABLED")
print("Timeframe:   1 minute")
print("Start time:  10:00 NY")
print("Data feed:   IEX")
print(f"Contracts:   {CONTRACTS}")
print("Orders:      ENABLED - PAPER ONLY")
print("========================================")


# ============================================
# STATE
# ============================================

one_minute_history = {
    ticker: []
    for ticker in TICKERS
}

# VWAP signal waiting for the NEXT candle.
pending_signals = {}

# Options opened by this bot.
open_trades = []

# Prevent duplicate candle processing.
last_processed_time = {}


# ============================================
# LOAD TODAY'S 1-MINUTE HISTORY
# ============================================

def load_today_history():

    now_ny = datetime.now(TIMEZONE)

    market_open = now_ny.replace(
        hour=9,
        minute=30,
        second=0,
        microsecond=0
    )

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
            columns={
                "symbol": "ticker"
            }
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
            f"{ticker:5s}: {count}"
        )

    return df


# ============================================
# CALCULATE EMA9 + SESSION VWAP
# ============================================

def calculate_indicators(df):

    df = df.copy()

    df = df.sort_values(
        "time"
    ).reset_index(drop=True)

    # ----------------------------------------
    # EMA9
    # Source = OPEN
    # ----------------------------------------

    df["ema9"] = (
        df["open"]
        .ewm(
            span=EMA_LENGTH,
            adjust=False
        )
        .mean()
    )

    # ----------------------------------------
    # SESSION VWAP
    # Source = OPEN
    # ----------------------------------------

    df["date"] = (
        df["time"].dt.date
    )

    df["open_volume"] = (
        df["open"]
        * df["volume"]
    )

    df["cumulative_open_volume"] = (
        df
        .groupby("date")["open_volume"]
        .cumsum()
    )

    df["cumulative_volume"] = (
        df
        .groupby("date")["volume"]
        .cumsum()
    )

    df["vwap"] = (
        df["cumulative_open_volume"]
        / df["cumulative_volume"]
    )

    return df


# ============================================
# FIND OPTION CONTRACT
# ============================================

def find_option_contract(
    ticker,
    direction,
    underlying_price,
    entry_date
):

    if direction == "LONG":
        option_type = ContractType.CALL
    else:
        option_type = ContractType.PUT

    try:

        request = GetOptionContractsRequest(
            underlying_symbols=[ticker],
            status=AssetStatus.ACTIVE,
            type=option_type,
            expiration_date_gte=entry_date,
            limit=10000,
        )

        response = trading_client.get_option_contracts(
            request
        )

        contracts = response.option_contracts

    except Exception as e:

        print()
        print("OPTION CONTRACT LOOKUP FAILED")
        print(f"{ticker}: {e}")

        return None

    if not contracts:

        print(
            f"NO OPTION CONTRACTS FOUND: {ticker}"
        )

        return None

    contracts = [
        c
        for c in contracts
        if c.tradable
    ]

    if not contracts:

        print(
            f"NO TRADABLE OPTIONS FOUND: {ticker}"
        )

        return None

    # ----------------------------------------
    # Nearest expiration
    # ----------------------------------------

    valid_expirations = sorted(
        set(
            c.expiration_date
            for c in contracts
            if c.expiration_date >= entry_date
        )
    )

    if not valid_expirations:

        print(
            f"NO VALID EXPIRATION: {ticker}"
        )

        return None

    nearest_expiration = (
        valid_expirations[0]
    )

    contracts = [
        c
        for c in contracts
        if c.expiration_date
        == nearest_expiration
    ]

    if not contracts:
        return None

    # ----------------------------------------
    # Strike closest to underlying
    # ----------------------------------------

    contract = min(
        contracts,
        key=lambda c: abs(
            float(c.strike_price)
            - underlying_price
        )
    )

    return contract


# ============================================
# SUBMIT PAPER ENTRY
# ============================================

def submit_paper_entry(
    ticker,
    direction,
    entry_time,
    entry_price,
    signal_ema9
):

    print()
    print("========================================")
    print("PAPER ENTRY")
    print("========================================")

    print(
        f"Ticker:          {ticker}"
    )

    print(
        f"Direction:       {direction}"
    )

    print(
        f"Entry time:      {entry_time}"
    )

    print(
        f"Underlying open: "
        f"{entry_price:.4f}"
    )

    print(
        f"Signal EMA9:     "
        f"{signal_ema9:.4f}"
    )

    contract = find_option_contract(
        ticker=ticker,
        direction=direction,
        underlying_price=entry_price,
        entry_date=entry_time.date()
    )

    if contract is None:

        print("ENTRY: REJECTED")
        print(
            "Reason: No tradable option."
        )
        print("========================================")

        return

    option_symbol = contract.symbol

    print(
        f"Option:          "
        f"{option_symbol}"
    )

    print(
        f"Strike:          "
        f"{float(contract.strike_price):.2f}"
    )

    print(
        f"Expiration:      "
        f"{contract.expiration_date}"
    )

    print(
        f"Contracts:       "
        f"{CONTRACTS}"
    )

    print("Order:            BUY")
    print("Mode:             PAPER")

    if not ORDERS_ENABLED:

        print(
            "ENTRY BLOCKED: orders disabled."
        )

        print("========================================")

        return

    try:

        order_request = MarketOrderRequest(
            symbol=option_symbol,
            qty=CONTRACTS,
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )

        order = trading_client.submit_order(
            order_request
        )

        print()
        print("PAPER ORDER SUBMITTED")

        print(
            f"Order ID:         "
            f"{order.id}"
        )

        print(
            f"Status:           "
            f"{order.status}"
        )

        open_trades.append(
            {
                "ticker": ticker,
                "direction": direction,
                "option_symbol": option_symbol,
                "qty": CONTRACTS,
                "entry_time": entry_time,
                "entry_underlying": entry_price,
                "entry_ema9": signal_ema9,
            }
        )

    except Exception as e:

        print()
        print("PAPER ORDER FAILED")
        print(str(e))

    print("========================================")


# ============================================
# CLOSE PAPER POSITION
# ============================================

def close_paper_trade(
    trade,
    reason,
    exit_time,
    underlying_close
):

    print()
    print("========================================")
    print("PAPER EXIT")
    print("========================================")

    print(
        f"Ticker:       "
        f"{trade['ticker']}"
    )

    print(
        f"Direction:    "
        f"{trade['direction']}"
    )

    print(
        f"Option:       "
        f"{trade['option_symbol']}"
    )

    print(
        f"Contracts:    "
        f"{trade['qty']}"
    )

    print(
        f"Exit time:    "
        f"{exit_time}"
    )

    print(
        f"Underlying:   "
        f"{underlying_close:.4f}"
    )

    print(
        f"Reason:       "
        f"{reason}"
    )

    try:

        close_request = ClosePositionRequest(
            qty=str(trade["qty"])
        )

        order = trading_client.close_position(
            symbol_or_asset_id=trade[
                "option_symbol"
            ],
            close_options=close_request
        )

        print(
            "PAPER EXIT SUBMITTED"
        )

        print(
            f"Order ID:     "
            f"{order.id}"
        )

        print(
            f"Status:       "
            f"{order.status}"
        )

        return True

    except Exception as e:

        print(
            "PAPER EXIT FAILED"
        )

        print(str(e))

        return False


# ============================================
# MANAGE EXISTING PAPER TRADES
# ============================================

def check_open_trades(
    ticker,
    current_candle
):

    if not open_trades:
        return

    current_time = (
        current_candle["time"]
    )

    current_close = float(
        current_candle["close"]
    )

    current_ema9 = float(
        current_candle["ema9"]
    )

    trades_to_remove = []

    for trade in open_trades:

        if trade["ticker"] != ticker:
            continue

        # Never exit on the entry candle.
        if current_time <= trade["entry_time"]:
            continue

        exit_reason = None

        # ------------------------------------
        # EMA9 EXIT
        # ------------------------------------

        if trade["direction"] == "LONG":

            if current_close < current_ema9:
                exit_reason = "EMA9"

        else:

            if current_close > current_ema9:
                exit_reason = "EMA9"

        # ------------------------------------
        # EOD EXIT
        # ------------------------------------

        if current_time.time() >= EOD_TIME:

            exit_reason = "EOD"

        if exit_reason is None:
            continue

        success = close_paper_trade(
            trade=trade,
            reason=exit_reason,
            exit_time=current_time,
            underlying_close=current_close
        )

        if success:

            trades_to_remove.append(
                trade
            )

    for trade in trades_to_remove:

        if trade in open_trades:

            open_trades.remove(
                trade
            )


# ============================================
# PROCESS COMPLETED 1-MINUTE CANDLE
# ============================================

def process_completed_candle(
    ticker,
    strategy_df
):

    if len(strategy_df) < 2:
        return

    current = strategy_df.iloc[-1]
    previous = strategy_df.iloc[-2]

    current_time = current["time"]

    # ----------------------------------------
    # Prevent duplicate processing
    # ----------------------------------------

    if (
        last_processed_time.get(ticker)
        == current_time
    ):

        return

    last_processed_time[ticker] = (
        current_time
    )

    # ----------------------------------------
    # Manage existing trades first
    # ----------------------------------------

    check_open_trades(
        ticker,
        current
    )

    # ----------------------------------------
    # CHECK PENDING SIGNAL
    # ----------------------------------------

    if ticker in pending_signals:

        signal = pending_signals.pop(
            ticker
        )

        direction = signal[
            "direction"
        ]

        signal_ema9 = float(
            signal["ema9"]
        )

        next_open = float(
            current["open"]
        )

        # ------------------------------------
        # ENTRY FILTER
        # ------------------------------------

        if direction == "LONG":

            valid = (
                next_open
                > signal_ema9
            )

        else:

            valid = (
                next_open
                < signal_ema9
            )

        print()
        print("========================================")
        print("NEXT-CANDLE ENTRY TEST")
        print("========================================")

        print(
            f"Ticker:          {ticker}"
        )

        print(
            f"Signal time:     "
            f"{signal['time']}"
        )

        print(
            f"Entry time:      "
            f"{current_time}"
        )

        print(
            f"Direction:       "
            f"{direction}"
        )

        print(
            f"Signal EMA9:     "
            f"{signal_ema9:.4f}"
        )

        print(
            f"Next candle OPEN: "
            f"{next_open:.4f}"
        )

        if direction == "LONG":

            print(
                f"Test:             "
                f"{next_open:.4f} > "
                f"{signal_ema9:.4f}"
            )

        else:

            print(
                f"Test:             "
                f"{next_open:.4f} < "
                f"{signal_ema9:.4f}"
            )

        if valid:

            print(
                "ENTRY:            VALID"
            )

            submit_paper_entry(
                ticker=ticker,
                direction=direction,
                entry_time=current_time,
                entry_price=next_open,
                signal_ema9=signal_ema9
            )

        else:

            print(
                "ENTRY:            REJECTED"
            )

            print(
                "Reason: EMA9 "
                "entry filter failed."
            )

        print(
            "========================================"
        )

    # ----------------------------------------
    # Do not create new signals before 10:00
    # ----------------------------------------

    if current_time.time() < START_TIME:
        return

    # ----------------------------------------
    # No new signals at/after EOD
    # ----------------------------------------

    if current_time.time() >= EOD_TIME:
        return

    # ----------------------------------------
    # VWAP CROSS
    # ----------------------------------------

    long_signal = (
        previous["close"]
        <= previous["vwap"]
        and
        current["close"]
        > current["vwap"]
    )

    short_signal = (
        previous["close"]
        >= previous["vwap"]
        and
        current["close"]
        < current["vwap"]
    )

    if not long_signal and not short_signal:
        return

    direction = (
        "LONG"
        if long_signal
        else "SHORT"
    )

    signal_ema9 = float(
        current["ema9"]
    )

    print()
    print("========================================")
    print("VWAP SIGNAL")
    print("========================================")

    print(
        f"Ticker:       {ticker}"
    )

    print(
        f"Signal time:  "
        f"{current_time}"
    )

    print(
        f"Direction:    "
        f"{direction}"
    )

    print(
        f"Close:        "
        f"{float(current['close']):.4f}"
    )

    print(
        f"VWAP:         "
        f"{float(current['vwap']):.4f}"
    )

    print(
        f"EMA9:         "
        f"{signal_ema9:.4f}"
    )

    print(
        "Waiting for NEXT 1-minute candle."
    )

    print(
        "========================================"
    )

    # ----------------------------------------
    # Save signal for NEXT candle
    # ----------------------------------------

    pending_signals[ticker] = {
        "time": current_time,
        "direction": direction,
        "ema9": signal_ema9,
    }


# ============================================
# LIVE BAR HANDLER
# ============================================

async def handle_bar(bar):

    ticker = bar.symbol

    if ticker not in one_minute_history:
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

    one_minute_history[
        ticker
    ].append(record)

    # Keep enough history for EMA9/VWAP.
    one_minute_history[
        ticker
    ] = one_minute_history[
        ticker
    ][-1000:]

    strategy_df = pd.DataFrame(
        one_minute_history[
            ticker
        ]
    )

    strategy_df = calculate_indicators(
        strategy_df
    )

    process_completed_candle(
        ticker,
        strategy_df
    )


# ============================================
# MAIN
# ============================================

async def main():

    # ----------------------------------------
    # 1. Load history
    # ----------------------------------------

    raw_data = load_today_history()

    # ----------------------------------------
    # 2. Calculate indicators PER TICKER
    #
    # Avoid pandas groupby.apply entirely.
    # ----------------------------------------

    strategy_data_parts = []

    for ticker in TICKERS:

        ticker_data = raw_data[
            raw_data["ticker"] == ticker
        ].copy()

        if ticker_data.empty:
            continue

        ticker_data = calculate_indicators(
            ticker_data
        )

        strategy_data_parts.append(
            ticker_data
        )

    if not strategy_data_parts:

        raise RuntimeError(
            "No strategy data was created."
        )

    strategy_data = pd.concat(
        strategy_data_parts,
        ignore_index=True
    )

    strategy_data = strategy_data.sort_values(
        ["ticker", "time"]
    ).reset_index(drop=True)

    print()
    print(
        "1-minute strategy data ready."
    )

    print(
        f"Rows: {len(strategy_data):,}"
    )

    # ----------------------------------------
    # 3. Seed history
    # ----------------------------------------

    for ticker in TICKERS:

        ticker_df = strategy_data[
            strategy_data["ticker"] == ticker
        ].copy()

        if ticker_df.empty:
            continue

        for _, row in ticker_df.iterrows():

            one_minute_history[
                ticker
            ].append(
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
    print(
        "1-minute history seeded."
    )

    for ticker in TICKERS:

        print(
            f"{ticker:5s}: "
            f"{len(one_minute_history[ticker])} "
            f"1-minute candles"
        )

    # ----------------------------------------
    # 4. LIVE IEX STREAM
    # ----------------------------------------

    stream = StockDataStream(
        API_KEY,
        API_SECRET,
        feed=DataFeed.IEX
    )

    print()
    print("========================================")
    print(
        "STARTING LIVE IEX 1-MINUTE STREAM"
    )
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
