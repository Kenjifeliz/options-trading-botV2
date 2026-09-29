import os
import asyncio
import sqlite3
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict

import pandas as pd

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from alpaca.data.live.stock import StockDataStream

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    GetOptionContractsRequest,
)
from alpaca.trading.enums import (
    OrderSide,
    TimeInForce,
    ContractType,
    AssetStatus,
)

try:
    from telegram_bot import (
        send_message,
        telegram_poll_loop,
    )
except Exception:
    send_message = None
    telegram_poll_loop = None


# ============================================================
# SETTINGS
# ============================================================

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

TIMEFRAME_MINUTES = 5

START_TIME = datetime.strptime(
    "10:00",
    "%H:%M"
).time()

FINAL_CANDLE_TIME = datetime.strptime(
    "15:55",
    "%H:%M"
).time()

CONTRACTS = 5

EMA_LENGTH = 9

VWAP_DISTANCE_THRESHOLD = 0.28

# ============================================================
# ACTUAL ORDER EXECUTION
#
# TRUE = submit orders to Alpaca.
#
# ALPACA_PAPER should remain TRUE while testing.
# ============================================================

ORDERS_ENABLED = (
    os.environ.get(
        "ORDERS_ENABLED",
        "true"
    ).lower()
    == "true"
)

PAPER_MODE = (
    os.environ.get(
        "ALPACA_PAPER",
        "true"
    ).lower()
    == "true"
)

TRADE_DB_PATH = os.environ.get(
    "TRADE_DB_PATH",
    "trade_history.db"
)


# ============================================================
# ALPACA CREDENTIALS
# ============================================================

API_KEY = os.environ["APCA_API_KEY_ID"]
API_SECRET = os.environ["APCA_API_SECRET_KEY"]


historical_client = StockHistoricalDataClient(
    API_KEY,
    API_SECRET,
)

trading_client = TradingClient(
    API_KEY,
    API_SECRET,
    paper=PAPER_MODE,
)


# ============================================================
# STARTUP
# ============================================================

print()
print("========================================")
print("OPTIONS TRADING BOT")
print("========================================")
print("Direction:       LONG ONLY")
print("Option:          CALL ONLY")
print("Timeframe:       5 minutes")
print("Start time:      10:00 NY")
print("Data feed:       IEX")
print(f"Contracts:       {CONTRACTS}")
print(
    f"VWAP filter:     "
    f"{VWAP_DISTANCE_THRESHOLD:.2f}%"
)
print(
    f"Orders:          "
    f"{'ENABLED' if ORDERS_ENABLED else 'DISABLED'}"
)
print(
    f"Alpaca paper:    "
    f"{'YES' if PAPER_MODE else 'NO'}"
)
print("========================================")
print()


# ============================================================
# TELEGRAM
# ============================================================

def notify(message):
    if send_message is None:
        return

    try:
        result = send_message(message)

        if asyncio.iscoroutine(result):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(result)
            except Exception:
                pass

    except Exception as exc:
        print(
            f"Telegram notification failed: {exc}"
        )


# ============================================================
# DATABASE
# ============================================================

def init_trade_database():

    conn = sqlite3.connect(
        TRADE_DB_PATH
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            option_symbol TEXT,
            option_type TEXT,
            strike REAL,
            expiration TEXT,
            contracts INTEGER,
            signal_time TEXT,
            entry_time TEXT,
            entry_underlying REAL,
            entry_option REAL,
            entry_order_id TEXT,
            exit_time TEXT,
            exit_underlying REAL,
            exit_option REAL,
            exit_order_id TEXT,
            exit_reason TEXT,
            pnl REAL,
            status TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()


def save_trade_entry(entry):

    conn = sqlite3.connect(
        TRADE_DB_PATH
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO trades (
            ticker,
            direction,
            option_symbol,
            option_type,
            strike,
            expiration,
            contracts,
            signal_time,
            entry_time,
            entry_underlying,
            entry_option,
            entry_order_id,
            status,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry["ticker"],
            entry["direction"],
            entry["option_symbol"],
            entry["option_type"],
            entry["strike"],
            str(entry["expiration"]),
            entry["contracts"],
            str(entry["signal_time"]),
            str(entry["entry_time"]),
            entry["entry_underlying"],
            entry["entry_option"],
            entry["entry_order_id"],
            "OPEN",
            datetime.now(TIMEZONE).isoformat(),
        ),
    )

    entry["db_id"] = cursor.lastrowid

    conn.commit()
    conn.close()


def save_trade_exit(entry):

    if not entry.get("db_id"):
        return

    conn = sqlite3.connect(
        TRADE_DB_PATH
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE trades
        SET
            exit_time = ?,
            exit_underlying = ?,
            exit_option = ?,
            exit_order_id = ?,
            exit_reason = ?,
            pnl = ?,
            status = ?
        WHERE id = ?
        """,
        (
            str(entry["exit_time"]),
            entry["exit_underlying"],
            entry["exit_option"],
            entry["exit_order_id"],
            entry["exit_reason"],
            entry["pnl"],
            "CLOSED",
            entry["db_id"],
        ),
    )

    conn.commit()
    conn.close()


def load_persistent_trades():

    conn = sqlite3.connect(
        TRADE_DB_PATH
    )

    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT *
        FROM trades
        ORDER BY id
        """
    ).fetchall()

    conn.close()

    trades = []

    for row in rows:

        trades.append(
            dict(row)
        )

    return trades


# ============================================================
# STATE
# ============================================================

five_minute_history = {
    ticker: []
    for ticker in TICKERS
}

one_minute_history = {
    ticker: []
    for ticker in TICKERS
}

active_entries = {
    ticker: []
    for ticker in TICKERS
}

pending_entries = {
    ticker: []
    for ticker in TICKERS
}

all_trades = []

last_processed_5m = {
    ticker: None
    for ticker in TICKERS
}

last_processed_open_bucket = {
    ticker: None
    for ticker in TICKERS
}

last_eod_date = {
    ticker: None
    for ticker in TICKERS
}


# ============================================================
# TIME HELPERS
# ============================================================

def ny_time(timestamp):

    ts = pd.Timestamp(timestamp)

    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")

    return ts.tz_convert(
        TIMEZONE
    )


def is_regular_session_time(timestamp):

    ts = ny_time(timestamp)

    if ts.weekday() >= 5:
        return False

    t = ts.time()

    return (
        datetime.strptime(
            "09:30",
            "%H:%M"
        ).time()
        <= t
        <= datetime.strptime(
            "16:00",
            "%H:%M"
        ).time()
    )


# ============================================================
# TODAY'S HISTORICAL 1-MINUTE DATA
# ============================================================

def load_today_history():

    now_ny = datetime.now(
        TIMEZONE
    )

    market_open = now_ny.replace(
        hour=9,
        minute=30,
        second=0,
        microsecond=0,
    )

    end_time = now_ny - timedelta(
        minutes=1
    )

    if end_time <= market_open:
        return pd.DataFrame()

    print(
        "Loading today's historical "
        "IEX bars..."
    )

    request = StockBarsRequest(
        symbol_or_symbols=TICKERS,
        timeframe=TimeFrame(
            1,
            TimeFrameUnit.Minute,
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
        return df

    if "symbol" in df.columns:
        df = df.rename(
            columns={
                "symbol": "ticker"
            }
        )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        utc=True,
    )

    df["timestamp"] = (
        df["timestamp"]
        .dt.tz_convert(TIMEZONE)
    )

    df = df.rename(
        columns={
            "timestamp": "time"
        }
    )

    return df[
        [
            "ticker",
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ].sort_values(
        ["ticker", "time"]
    ).reset_index(
        drop=True
    )


# ============================================================
# BUILD 5-MINUTE HISTORY
# ============================================================

def prepare_strategy_data(raw):

    results = []

    if raw.empty:
        return pd.DataFrame()

    for ticker in TICKERS:

        ticker_df = raw[
            raw["ticker"] == ticker
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
            origin="start_day",
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
                "close",
            ]
        )

        if five.empty:
            continue

        # EMA9
        # TradingView setting:
        # Length = 9
        # Source = OPEN
        # Offset = 0

        five["ema9"] = (
            five["open"]
            .ewm(
                span=EMA_LENGTH,
                adjust=False,
            )
            .mean()
        )

        # Session VWAP
        # TradingView setting:
        # Anchor = Session
        # Source = OPEN

        five["session_date"] = (
            five.index.date
        )

        five["open_volume"] = (
            five["open"]
            * five["volume"]
        )

        five["cum_open_volume"] = (
            five.groupby(
                "session_date"
            )["open_volume"]
            .cumsum()
        )

        five["cum_volume"] = (
            five.groupby(
                "session_date"
            )["volume"]
            .cumsum()
        )

        five["vwap"] = (
            five["cum_open_volume"]
            / five["cum_volume"]
        )

        five = five.reset_index()

        results.append(
            five[
                [
                    "ticker",
                    "time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "ema9",
                    "vwap",
                ]
            ]
        )

    if not results:
        return pd.DataFrame()

    return pd.concat(
        results,
        ignore_index=True,
    ).sort_values(
        ["ticker", "time"]
    ).reset_index(
        drop=True
    )


def seed_history(five_df):

    if five_df.empty:
        return

    for ticker in TICKERS:

        ticker_df = five_df[
            five_df["ticker"] == ticker
        ]

        for _, row in ticker_df.iterrows():

            five_minute_history[
                ticker
            ].append(
                {
                    "ticker": ticker,
                    "time": pd.Timestamp(
                        row["time"]
                    ),
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

        five_minute_history[
            ticker
        ] = (
            five_minute_history[
                ticker
            ][-500:]
        )


# ============================================================
# CALCULATE STRATEGY VALUES
# ============================================================

def build_strategy_df(ticker):

    rows = five_minute_history[
        ticker
    ]

    if len(rows) < 3:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows
    ).sort_values(
        "time"
    ).reset_index(
        drop=True
    )

    df["ema9"] = (
        df["open"]
        .ewm(
            span=EMA_LENGTH,
            adjust=False,
        )
        .mean()
    )

    df["session_date"] = (
        df["time"].dt.date
    )

    df["open_volume"] = (
        df["open"]
        * df["volume"]
    )

    df["cum_open_volume"] = (
        df.groupby(
            "session_date"
        )["open_volume"]
        .cumsum()
    )

    df["cum_volume"] = (
        df.groupby(
            "session_date"
        )["volume"]
        .cumsum()
    )

    df["vwap"] = (
        df["cum_open_volume"]
        / df["cum_volume"]
    )

    return df


# ============================================================
# OPTION EXPIRATION
#
# Established setup:
# nearest Friday expiration.
#
# This allows 0DTE on Friday and the nearest weekly
# expiration on other weekdays.
# ============================================================

def nearest_friday(signal_date):

    days_until_friday = (
        4 - signal_date.weekday()
    ) % 7

    return (
        signal_date
        + timedelta(
            days=days_until_friday
        )
    )


# ============================================================
# OPTION CONTRACT SELECTION
#
# Long signal -> CALL
# Strike -> nearest available ATM strike
# Expiration -> nearest Friday
# ============================================================

def select_call_contract(
    ticker,
    underlying_price,
    signal_date,
):

    expiration = nearest_friday(
        signal_date
    )

    request = GetOptionContractsRequest(
        underlying_symbols=[ticker],
        status=AssetStatus.ACTIVE,
        expiration_date=expiration,
        type=ContractType.CALL,
        limit=10000,
    )

    response = (
        trading_client
        .get_option_contracts(
            request
        )
    )

    contracts = (
        response.option_contracts
        or []
    )

    candidates = []

    for contract in contracts:

        try:
            strike = float(
                contract.strike_price
            )
        except Exception:
            continue

        if not contract.tradable:
            continue

        if contract.type != ContractType.CALL:
            continue

        if (
            contract.expiration_date
            != expiration
        ):
            continue

        candidates.append(
            (
                abs(
                    strike
                    - underlying_price
                ),
                strike,
                contract,
            )
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x[0],
            x[1],
        )
    )

    return candidates[0][2]


# ============================================================
# WAIT FOR ORDER FILL
# ============================================================

def wait_for_order_fill(
    order_id,
    timeout_seconds=30,
):

    start = datetime.now()

    while (
        datetime.now()
        - start
    ).total_seconds() < timeout_seconds:

        try:
            order = (
                trading_client
                .get_order_by_id(
                    order_id
                )
            )

            status = str(
                order.status
            ).lower()

            if "filled" in status:
                return order

            if (
                "canceled" in status
                or "rejected" in status
                or "expired" in status
            ):
                return order

        except Exception as exc:

            print(
                "Order status check failed:",
                exc,
            )

        import time
        time.sleep(1)

    return None


# ============================================================
# OPTION ENTRY
# ============================================================

def execute_option_entry(
    ticker,
    signal_time,
    entry_time,
    underlying_open,
    signal_close,
    signal_vwap,
    signal_ema9,
    distance_pct,
):

    if not ORDERS_ENABLED:

        print(
            f"{ticker}: valid LONG signal "
            f"but orders are disabled."
        )

        return None

    try:

        contract = (
            select_call_contract(
                ticker,
                underlying_open,
                signal_time.date(),
            )
        )

    except Exception as exc:

        print(
            f"{ticker}: option lookup failed: "
            f"{exc}"
        )

        notify(
            f"⚠️ {ticker} LONG signal\n"
            f"CALL contract lookup failed:\n"
            f"{exc}"
        )

        return None

    if contract is None:

        print(
            f"{ticker}: no suitable CALL "
            f"contract found."
        )

        notify(
            f"⚠️ {ticker} LONG signal\n"
            f"No suitable CALL contract "
            f"was available."
        )

        return None

    option_symbol = (
        contract.symbol
    )

    strike = float(
        contract.strike_price
    )

    expiration = (
        contract.expiration_date
    )

    print()
    print(
        "========================================"
    )
    print("SUBMITTING OPTION ENTRY")
    print(
        "========================================"
    )
    print(
        f"Ticker:       {ticker}"
    )
    print(
        f"Direction:    LONG"
    )
    print(
        f"Option:       CALL"
    )
    print(
        f"Symbol:       {option_symbol}"
    )
    print(
        f"Strike:       {strike:.2f}"
    )
    print(
        f"Expiration:   {expiration}"
    )
    print(
        f"Contracts:    {CONTRACTS}"
    )
    print(
        f"Underlying:   {underlying_open:.4f}"
    )
    print(
        "========================================"
    )

    try:

        order_request = (
            MarketOrderRequest(
                symbol=option_symbol,
                qty=CONTRACTS,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
            )
        )

        order = (
            trading_client
            .submit_order(
                order_data=order_request
            )
        )

    except Exception as exc:

        print(
            f"{ticker}: BUY order failed: "
            f"{exc}"
        )

        notify(
            f"❌ OPTION ENTRY FAILED\n"
            f"{ticker} CALL\n"
            f"{option_symbol}\n"
            f"Error: {exc}"
        )

        return None

    filled_order = wait_for_order_fill(
        order.id
    )

    if filled_order is None:

        print(
            f"{ticker}: order did not fill "
            f"within timeout."
        )

        notify(
            f"⚠️ {ticker} CALL order\n"
            f"Order submitted but fill "
            f"was not confirmed within "
            f"30 seconds."
        )

        return None

    status = str(
        filled_order.status
    ).lower()

    if "filled" not in status:

        notify(
            f"⚠️ {ticker} CALL order\n"
            f"Order status: {status}"
        )

        return None

    filled_qty = int(
        float(
            filled_order.filled_qty
            or CONTRACTS
        )
    )

    filled_price = None

    if filled_order.filled_avg_price:

        filled_price = float(
            filled_order.filled_avg_price
        )

    entry = {
        "ticker": ticker,
        "direction": "LONG",
        "option_symbol": option_symbol,
        "option_type": "CALL",
        "strike": strike,
        "expiration": expiration,
        "contracts": filled_qty,
        "signal_time": signal_time,
        "entry_time": entry_time,
        "entry_underlying": float(
            underlying_open
        ),
        "entry_option": filled_price,
        "entry_order_id": str(
            order.id
        ),
        "signal_close": float(
            signal_close
        ),
        "signal_vwap": float(
            signal_vwap
        ),
        "signal_ema9": float(
            signal_ema9
        ),
        "vwap_distance_pct": float(
            distance_pct
        ),
        "exit_time": None,
        "exit_underlying": None,
        "exit_option": None,
        "exit_order_id": None,
        "exit_reason": None,
        "pnl": None,
    }

    active_entries[
        ticker
    ].append(entry)

    all_trades.append(
        entry
    )

    save_trade_entry(
        entry
    )

    price_text = (
        f"${filled_price:.2f}"
        if filled_price is not None
        else "N/A"
    )

    notify(
        f"🟢 OPTION ENTRY\n"
        f"{ticker} — LONG CALL\n"
        f"Contract: {option_symbol}\n"
        f"Strike: ${strike:.2f}\n"
        f"Expiration: {expiration}\n"
        f"Contracts: {filled_qty}\n"
        f"Fill: {price_text}\n"
        f"Underlying: ${underlying_open:.2f}"
    )

    return entry


# ============================================================
# VALID SIGNAL
# ============================================================

def queue_valid_signal(
    ticker,
    previous,
    signal,
):

    previous_time = pd.Timestamp(
        previous["time"]
    )

    signal_time = pd.Timestamp(
        signal["time"]
    )

    if (
        previous_time.date()
        != signal_time.date()
    ):
        return

    # Ignore everything before 10:00.
    if (
        signal_time.time()
        < START_TIME
    ):
        return

    # Never create a new signal on
    # the final candle.
    if (
        signal_time.time()
        >= FINAL_CANDLE_TIME
    ):
        return

    # --------------------------------------------------------
    # LONG VWAP CROSS ONLY
    # --------------------------------------------------------

    long_cross = (
        float(previous["close"])
        <= float(previous["vwap"])
        and
        float(signal["close"])
        > float(signal["vwap"])
    )

    if not long_cross:
        return

    signal_close = float(
        signal["close"]
    )

    signal_vwap = float(
        signal["vwap"]
    )

    if signal_vwap == 0:
        return

    distance_pct = (
        abs(
            signal_close
            - signal_vwap
        )
        / abs(signal_vwap)
        * 100
    )

    # --------------------------------------------------------
    # VWAP DISTANCE
    # --------------------------------------------------------

    if (
        distance_pct
        < VWAP_DISTANCE_THRESHOLD
    ):
        return

    # --------------------------------------------------------
    # SIGNAL CANDLE MUST CLOSE ABOVE EMA9
    # --------------------------------------------------------

    signal_ema9 = float(
        signal["ema9"]
    )

    if signal_close <= signal_ema9:
        return

    # --------------------------------------------------------
    # NO DUPLICATE SIGNAL
    # --------------------------------------------------------

    if any(
        pd.Timestamp(
            p["signal_time"]
        )
        == signal_time
        for p in pending_entries[ticker]
    ):
        return

    pending = {
        "signal_time": signal_time,
        "signal_close": signal_close,
        "signal_vwap": signal_vwap,
        "signal_ema9": signal_ema9,
        "distance_pct": distance_pct,
    }

    pending_entries[
        ticker
    ].append(
        pending
    )

    print()
    print(
        "========================================"
    )
    print("VALID LONG SIGNAL")
    print(
        "========================================"
    )
    print(
        f"Ticker:          {ticker}"
    )
    print(
        f"Signal time:     {signal_time}"
    )
    print(
        f"Previous close:  "
        f"{float(previous['close']):.4f}"
    )
    print(
        f"Previous VWAP:   "
        f"{float(previous['vwap']):.4f}"
    )
    print(
        f"Signal close:    "
        f"{signal_close:.4f}"
    )
    print(
        f"Signal VWAP:     "
        f"{signal_vwap:.4f}"
    )
    print(
        f"VWAP distance:   "
        f"{distance_pct:.4f}%"
    )
    print(
        f"Signal EMA9:     "
        f"{signal_ema9:.4f}"
    )
    print(
        "Next candle:     WAITING FOR OPEN"
    )
    print(
        "========================================"
    )

    notify(
        f"📡 VALID LONG SIGNAL\n"
        f"{ticker}\n"
        f"Signal: "
        f"{signal_time.strftime('%H:%M')} ET\n"
        f"VWAP: ${signal_vwap:.2f}\n"
        f"Close: ${signal_close:.2f}\n"
        f"EMA9: ${signal_ema9:.2f}\n"
        f"VWAP distance: {distance_pct:.2f}%\n"
        f"Waiting for next candle OPEN."
    )


# ============================================================
# NEXT CANDLE OPEN
# ============================================================

def execute_pending_open(
    ticker,
    timestamp,
    open_price,
):

    if not pending_entries[
        ticker
    ]:
        return

    bucket = pd.Timestamp(
        timestamp
    ).floor(
        "5min"
    )

    if (
        last_processed_open_bucket[
            ticker
        ]
        == bucket
    ):
        return

    matching = [
        p
        for p in pending_entries[
            ticker
        ]
        if pd.Timestamp(
            p["signal_time"]
        ).floor(
            "5min"
        ) < bucket
    ]

    if not matching:
        return

    last_processed_open_bucket[
        ticker
    ] = bucket

    remaining = []

    for pending in matching:

        signal_ema9 = float(
            pending["signal_ema9"]
        )

        # EXACT RULE:
        #
        # Next candle OPEN must be
        # above signal candle EMA9.

        if (
            float(open_price)
            <= signal_ema9
        ):

            print(
                f"{ticker}: LONG rejected "
                f"at next open."
            )

            notify(
                f"⛔ {ticker} LONG rejected\n"
                f"Next open: "
                f"${float(open_price):.2f}\n"
                f"Signal EMA9: "
                f"${signal_ema9:.2f}"
            )

            continue

        execute_option_entry(
            ticker=ticker,
            signal_time=pd.Timestamp(
                pending["signal_time"]
            ),
            entry_time=bucket,
            underlying_open=float(
                open_price
            ),
            signal_close=float(
                pending["signal_close"]
            ),
            signal_vwap=float(
                pending["signal_vwap"]
            ),
            signal_ema9=signal_ema9,
            distance_pct=float(
                pending["distance_pct"]
            ),
        )

    # Pending signals only live for
    # their immediate next candle.
    for p in pending_entries[
        ticker
    ]:

        if p not in matching:
            remaining.append(p)

    pending_entries[
        ticker
    ] = remaining


# ============================================================
# OPTION EXIT
#
# IMPORTANT:
# We SELL the exact number of contracts belonging
# to this strategy entry.
#
# We do NOT use close_position(), because overlapping
# entries may use the same option contract.
# ============================================================

def execute_option_exit(
    entry,
    current_time,
    underlying_price,
    reason,
):

    symbol = entry[
        "option_symbol"
    ]

    qty = int(
        entry["contracts"]
    )

    try:

        order_request = (
            MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
            )
        )

        order = (
            trading_client
            .submit_order(
                order_data=order_request
            )
        )

    except Exception as exc:

        print(
            f"{entry['ticker']}: "
            f"EXIT ORDER FAILED: {exc}"
        )

        notify(
            f"❌ OPTION EXIT FAILED\n"
            f"{entry['ticker']}\n"
            f"{symbol}\n"
            f"Reason: {reason}\n"
            f"Error: {exc}"
        )

        return False

    filled_order = wait_for_order_fill(
        order.id
    )

    if filled_order is None:

        notify(
            f"⚠️ {entry['ticker']} EXIT\n"
            f"{symbol}\n"
            f"Exit order submitted but "
            f"fill not confirmed."
        )

        return False

    status = str(
        filled_order.status
    ).lower()

    if "filled" not in status:

        notify(
            f"⚠️ {entry['ticker']} EXIT\n"
            f"{symbol}\n"
            f"Order status: {status}"
        )

        return False

    exit_price = None

    if filled_order.filled_avg_price:

        exit_price = float(
            filled_order.filled_avg_price
        )

    entry_price = (
        entry["entry_option"]
    )

    pnl = None

    if (
        entry_price is not None
        and exit_price is not None
    ):

        pnl = (
            exit_price
            - entry_price
        ) * qty * 100

    entry["exit_time"] = (
        current_time
    )

    entry["exit_underlying"] = (
        float(underlying_price)
    )

    entry["exit_option"] = (
        exit_price
    )

    entry["exit_order_id"] = (
        str(order.id)
    )

    entry["exit_reason"] = (
        reason
    )

    entry["pnl"] = pnl

    save_trade_exit(
        entry
    )

    pnl_text = (
        f"${pnl:+.2f}"
        if pnl is not None
        else "N/A"
    )

    exit_text = (
        f"${exit_price:.2f}"
        if exit_price is not None
        else "N/A"
    )

    notify(
        f"🔴 OPTION EXIT\n"
        f"{entry['ticker']} — LONG CALL\n"
        f"Contract: {symbol}\n"
        f"Contracts: {qty}\n"
        f"Exit: {exit_text}\n"
        f"P&L: {pnl_text}\n"
        f"Reason: {reason}"
    )

    return True


# ============================================================
# EMA9 EXIT
# ============================================================

def check_ema9_exits(
    ticker,
    candle,
    strategy_df,
):

    if not active_entries[
        ticker
    ]:
        return

    current_time = pd.Timestamp(
        candle["time"]
    )

    current_close = float(
        candle["close"]
    )

    current_ema9 = float(
        strategy_df.iloc[-1]["ema9"]
    )

    remaining = []

    for entry in active_entries[
        ticker
    ]:

        entry_time = pd.Timestamp(
            entry["entry_time"]
        )

        # Never carry overnight.
        if (
            entry_time.date()
            != current_time.date()
        ):

            remaining.append(
                entry
            )
            continue

        # The entry candle cannot
        # trigger its own exit.
        if current_time <= entry_time:

            remaining.append(
                entry
            )
            continue

        # LONG ONLY:
        #
        # Exit only when a completed
        # candle CLOSES below EMA9.
        #
        # Wicks are ignored.

        if (
            current_close
            < current_ema9
        ):

            print()
            print(
                "========================================"
            )
            print("EMA9 EXIT")
            print(
                "========================================"
            )
            print(
                f"Ticker:       {ticker}"
            )
            print(
                f"Option:       "
                f"{entry['option_symbol']}"
            )
            print(
                f"Close:        "
                f"{current_close:.4f}"
            )
            print(
                f"EMA9:         "
                f"{current_ema9:.4f}"
            )
            print(
                f"Exit time:    {current_time}"
            )
            print(
                "========================================"
            )

            success = (
                execute_option_exit(
                    entry,
                    current_time,
                    current_close,
                    "EMA9",
                )
            )

            if not success:
                remaining.append(
                    entry
                )

        else:

            remaining.append(
                entry
            )

    active_entries[
        ticker
    ] = remaining


# ============================================================
# END OF DAY
#
# Final available regular-session 5-minute candle
# is 15:55 -> closes around 16:00.
# ============================================================

def check_end_of_day(
    ticker,
    candle,
):

    current_time = pd.Timestamp(
        candle["time"]
    )

    if (
        current_time.time()
        != FINAL_CANDLE_TIME
    ):
        return

    if (
        last_eod_date[ticker]
        == current_time.date()
    ):
        return

    last_eod_date[
        ticker
    ] = current_time.date()

    # No new entry may come from
    # the final candle.
    pending_entries[
        ticker
    ] = []

    if not active_entries[
        ticker
    ]:
        return

    current_close = float(
        candle["close"]
    )

    print()
    print(
        "========================================"
    )
    print("END OF DAY EXIT")
    print(
        "========================================"
    )
    print(
        f"Ticker: {ticker}"
    )
    print(
        f"Positions: "
        f"{len(active_entries[ticker])}"
    )
    print(
        f"Underlying close: "
        f"{current_close:.4f}"
    )
    print(
        "========================================"
    )

    remaining = []

    for entry in list(
        active_entries[ticker]
    ):

        success = (
            execute_option_exit(
                entry,
                current_time,
                current_close,
                "END_OF_DAY",
            )
        )

        if not success:
            remaining.append(
                entry
            )

    active_entries[
        ticker
    ] = remaining


# ============================================================
# COMPLETED 5-MINUTE CANDLE
# ============================================================

def process_completed_5m_candle(
    ticker,
    candle,
):

    timestamp = pd.Timestamp(
        candle["time"]
    )

    if (
        last_processed_5m[ticker]
        == timestamp
    ):
        return

    last_processed_5m[
        ticker
    ] = timestamp

    five_minute_history[
        ticker
    ].append(
        candle
    )

    five_minute_history[
        ticker
    ] = (
        five_minute_history[
            ticker
        ][-500:]
    )

    strategy_df = (
        build_strategy_df(
            ticker
        )
    )

    if strategy_df.empty:
        return

    # --------------------------------------------------------
    # EXIT FIRST
    # --------------------------------------------------------

    check_ema9_exits(
        ticker,
        candle,
        strategy_df,
    )

    # --------------------------------------------------------
    # EOD
    # --------------------------------------------------------

    check_end_of_day(
        ticker,
        candle,
    )

    # --------------------------------------------------------
    # NEVER ENTER ON FINAL CANDLE
    # --------------------------------------------------------

    if (
        timestamp.time()
        >= FINAL_CANDLE_TIME
    ):
        return

    if len(strategy_df) < 2:
        return

    previous = (
        strategy_df.iloc[-2]
    )

    signal = (
        strategy_df.iloc[-1]
    )

    queue_valid_signal(
        ticker,
        previous,
        signal,
    )


# ============================================================
# 1-MINUTE BAR HANDLER
# ============================================================

async def handle_bar(bar):

    ticker = bar.symbol

    if ticker not in one_minute_history:
        return

    timestamp = ny_time(
        bar.timestamp
    )

    if not is_regular_session_time(
        timestamp
    ):
        return

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
    ].append(
        record
    )

    # Keep enough recent bars
    # to construct one 5-minute candle.
    one_minute_history[
        ticker
    ] = (
        one_minute_history[
            ticker
        ][-10:]
    )

    # Only process the completed
    # 5-minute candle at :04, :09,
    # :14, :19, etc.
    if (
        timestamp.minute % 5
        != 4
    ):
        return

    bucket = timestamp.floor(
        "5min"
    )

    bars = [
        x
        for x in one_minute_history[
            ticker
        ]
        if pd.Timestamp(
            x["time"]
        ).floor("5min")
        == bucket
    ]

    if len(bars) < 5:
        return

    bars = sorted(
        bars,
        key=lambda x: x["time"]
    )

    candle = {
        "ticker": ticker,
        "time": bucket,
        "open": float(
            bars[0]["open"]
        ),
        "high": max(
            float(x["high"])
            for x in bars
        ),
        "low": min(
            float(x["low"])
            for x in bars
        ),
        "close": float(
            bars[-1]["close"]
        ),
        "volume": sum(
            float(x["volume"])
            for x in bars
        ),
    }

    process_completed_5m_candle(
        ticker,
        candle,
    )


# ============================================================
# TRADE HANDLER
#
# Used to capture the first trade observed in
# the NEXT 5-minute bucket.
#
# That is the price used for the strategy's
# next-candle OPEN check.
# ============================================================

async def handle_trade(
    trade
):

    ticker = trade.symbol

    if ticker not in pending_entries:
        return

    if not pending_entries[
        ticker
    ]:
        return

    timestamp = ny_time(
        trade.timestamp
    )

    if not is_regular_session_time(
        timestamp
    ):
        return

    bucket = timestamp.floor(
        "5min"
    )

    if (
        last_processed_open_bucket[
            ticker
        ]
        == bucket
    ):
        return

    execute_pending_open(
        ticker,
        timestamp,
        float(trade.price),
    )


# ============================================================
# STARTUP TELEGRAM
# ============================================================

def notify_startup():

    notify(
        "🟢 BOT ONLINE\n"
        "Mode: "
        + (
            "PAPER"
            if PAPER_MODE
            else "LIVE"
        )
        + "\n"
        "Direction: LONG ONLY\n"
        "Option: CALL ONLY\n"
        f"Contracts: {CONTRACTS}\n"
        "Timeframe: 5m\n"
        "Start: 10:00 NY\n"
        "Orders: "
        + (
            "ENABLED"
            if ORDERS_ENABLED
            else "DISABLED"
        )
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    init_trade_database()

    global all_trades

    all_trades = (
        load_persistent_trades()
    )

    print(
        f"Persistent trades loaded: "
        f"{len(all_trades)}"
    )

    # --------------------------------------------------------
    # LOAD TODAY'S HISTORY
    # --------------------------------------------------------

    try:

        raw = (
            load_today_history()
        )

        if not raw.empty:

            five = (
                prepare_strategy_data(
                    raw
                )
            )

            seed_history(
                five
            )

            print(
                "Strategy history seeded."
            )

            for ticker in TICKERS:

                print(
                    f"{ticker:5s}: "
                    f"{len(five_minute_history[ticker])} "
                    f"5m candles"
                )

    except Exception as exc:

        print(
            "Historical data load failed:"
        )
        print(exc)

        notify(
            f"⚠️ Historical data load "
            f"failed:\n{exc}"
        )

    # --------------------------------------------------------
    # TELEGRAM
    # --------------------------------------------------------

    if telegram_poll_loop is not None:

        try:

            telegram_state = {
                "active_entries":
                    active_entries,

                "all_trades":
                    all_trades,

                "five_minute_history":
                    five_minute_history,

            }

            asyncio.create_task(
                telegram_poll_loop(
                    telegram_state
                )
            )

            print(
                "Telegram command listener "
                "started."
            )

        except Exception as exc:

            print(
                "Telegram listener failed:"
            )
            print(exc)

    # --------------------------------------------------------
    # LIVE STREAM
    # --------------------------------------------------------

    stream = StockDataStream(
        API_KEY,
        API_SECRET,
        feed=DataFeed.IEX,
    )

    for ticker in TICKERS:

        stream.subscribe_bars(
            handle_bar,
            ticker,
        )

        stream.subscribe_trades(
            handle_trade,
            ticker,
        )

    notify_startup()

    print()
    print(
        "========================================"
    )
    print(
        "LIVE 5-MINUTE IEX STREAM STARTED"
    )
    print(
        "========================================"
    )
    print(
        "Direction: LONG ONLY"
    )
    print(
        "Option: CALL ONLY"
    )
    print(
        f"Contracts: {CONTRACTS}"
    )
    print(
        "Start: 10:00 NY"
    )
    print(
        "VWAP distance: 0.28%"
    )
    print(
        "EMA9: OPEN source"
    )
    print(
        "VWAP: OPEN source"
    )
    print(
        "Entry: NEXT 5m OPEN"
    )
    print(
        "Exit: CLOSE below EMA9"
    )
    print(
        "EOD: 15:55 candle"
    )
    print(
        "Overnight: DISABLED"
    )
    print(
        "Orders: "
        + (
            "ENABLED"
            if ORDERS_ENABLED
            else "DISABLED"
        )
    )
    print(
        "Alpaca paper: "
        + (
            "YES"
            if PAPER_MODE
            else "NO"
        )
    )
    print(
        "========================================"
    )

    await stream._run_forever()


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "Bot stopped."
        )

    except Exception as exc:

        print(
            "BOT CRASHED:"
        )

        print(exc)

        notify(
            f"🔴 BOT CRASHED\n"
            f"{exc}"
        )

        raise
