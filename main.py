import os
import re
import time
import asyncio
import logging
import sqlite3
import functools
import threading
import traceback
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from alpaca.data.live.stock import StockDataStream

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    GetOptionContractsRequest,
    GetOrdersRequest,
)
from alpaca.trading.enums import (
    OrderSide,
    TimeInForce,
    ContractType,
    AssetStatus,
    AssetClass,
    QueryOrderStatus,
)

try:
    from telegram_bot import (
        send_message,
        telegram_poll_loop,
    )
except Exception:
    send_message = None
    telegram_poll_loop = None


# Railway block-buffers stdout when it is not a terminal, so a frozen process can hide its last
# log lines. Flush every print immediately.
print = functools.partial(print, flush=True)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
# alpaca-py reports stream subscriptions and server errors (for example "symbol limit
# exceeded") through logging; INFO makes those confirmations visible in the Railway logs.
logging.getLogger("alpaca").setLevel(logging.INFO)


# ============================================================
# SETTINGS
# ============================================================

# V3 Giveback active universe: master 65 tickers minus the 14 explicitly removed.
TICKERS = [
    "AAL", "AAPL", "AMD", "AMZN", "ASTS", "AUR", "BABA", "BAC",
    "BE", "CCL", "COIN", "CSCO", "ET", "F", "GME", "GOOG",
    "GOOGL", "GRAB", "HIMS", "HOOD", "HPE", "INTC", "IREN", "JD",
    "JPM", "LI", "MARA", "META", "MRVL", "MSFT", "MSTR", "MU",
    "NBIS", "NIO", "NOK", "NVDA", "ORCL", "PFE", "PLTR", "QQQ",
    "SNAP", "SOFI", "SOUN", "SPCX", "SPY", "T", "TSLA", "TSM",
    "UAL", "VALE", "XPEV",
]
REMOVED_TICKERS = [
    "AVGO", "BTDR", "JBLU", "LCID", "OPEN", "OXY", "PLUG",
    "QCOM", "RIOT", "RKLB", "SMCI", "UBER", "VZ", "RIVN",
]
TIMEZONE = ZoneInfo("America/New_York")
# Track excluded tickers only when necessary to manage/reconcile legacy open positions.
MANAGED_TICKERS = sorted(set(TICKERS) | set(REMOVED_TICKERS))
TIMEFRAME_MINUTES = 1
EMA_LENGTH = 9
ATR_LENGTH = 14
GIVEBACK_ACTIVATION_ATR = 1.5
GIVEBACK_FRACTION = 0.35
# Thresholds are backtest variants. By default one order is placed per signal candle,
# using the highest configured threshold met; set true only if deliberately trading each
# threshold variant as a separate overlapping position.
PARALLEL_THRESHOLD_TRADES = os.environ.get("V3_PARALLEL_THRESHOLD_TRADES", "false").lower() == "true"
FINAL_CANDLE_TIME = datetime.strptime("15:59", "%H:%M").time()
CONTRACTS = int(os.environ.get("CONTRACTS", "5"))

# ------------------------------------------------------------
# Execution-safety settings. None of these change the V3 signal or exit rules.
# ------------------------------------------------------------
# The strategy's end-of-day exit is the close of the 15:59 candle (= 16:00:00). Single-stock options
# stop trading at 16:00 ET, so an order sent only after that candle completes can miss the market.
# The bot therefore flattens open positions this many seconds before the close. 0 disables it.
EOD_FLATTEN_SECONDS_BEFORE_CLOSE = int(os.environ.get("EOD_FLATTEN_SECONDS_BEFORE_CLOSE", "20"))
# If the asyncio loop makes no progress for this long, alert on Telegram and exit so Railway restarts.
WATCHDOG_STALL_SECONDS = int(os.environ.get("WATCHDOG_STALL_SECONDS", "300"))
# How long to keep asking REST for the true first trade of the entry minute.
ENTRY_OPEN_REST_TIMEOUT_SECONDS = int(os.environ.get("ENTRY_OPEN_REST_TIMEOUT_SECONDS", "20"))
# Seconds after each minute boundary at which the REST gap-filler looks for missing bars.
GAPFILL_DELAY_SECONDS = int(os.environ.get("GAPFILL_DELAY_SECONDS", "6"))
# A position younger than this is never declared "closed outside the bot" by reconciliation.
RECONCILE_GRACE_SECONDS = 120

# Configured threshold variants run independently, as in the strategy register.
# Threshold values are decimals (0.003 = 0.30%).
DEFAULT_ENTRY_START = "10:00"
DEFAULT_ENTRY_END = "15:55"
DEFAULT_THRESHOLDS = (0.0030, 0.0035, 0.0040)
CUSTOM_CONFIG = {
    "AMD":  {"start": "11:45", "end": "14:45", "thresholds": (0.0030, 0.0035, 0.0040)},
    "INTC": {"start": "14:30", "end": "15:45", "thresholds": (0.0030, 0.0035, 0.0040)},
    "IREN": {"start": "11:30", "end": "15:55", "thresholds": (0.0030, 0.0035)},
    "PLTR": {"start": "10:50", "end": "14:00", "thresholds": (0.0030,)},
    "ASTS": {"start": "10:45", "end": "12:00", "thresholds": (0.0035,)},
    "BE":   {"start": "10:30", "end": "11:00", "thresholds": (0.0040,)},
    "GRAB": {"start": "11:45", "end": "13:15", "thresholds": (0.0040,)},
    "MARA": {"start": "10:00", "end": "11:00", "thresholds": (0.0030,)},
}

EXPECTED_REMOVED = {"AVGO", "BTDR", "JBLU", "LCID", "OPEN", "OXY", "PLUG", "QCOM", "RIOT", "RKLB", "SMCI", "UBER", "VZ", "RIVN"}
if set(TICKERS) & EXPECTED_REMOVED:
    raise RuntimeError(f"Excluded tickers accidentally active: {sorted(set(TICKERS) & EXPECTED_REMOVED)}")
if len(TICKERS) != 51 or len(set(TICKERS)) != 51:
    raise RuntimeError(f"V3 active universe should contain 51 unique tickers; found {len(TICKERS)}")

def ticker_config(ticker):
    cfg = CUSTOM_CONFIG.get(ticker)
    if cfg is None:
        return {"start": DEFAULT_ENTRY_START, "end": DEFAULT_ENTRY_END,
                "thresholds": DEFAULT_THRESHOLDS, "source": "standard"}
    return {**cfg, "source": "custom"}

def _clock(value):
    return datetime.strptime(value, "%H:%M").time()

# Free Alpaca Basic market-data default: IEX. SIP requires a paid data entitlement.
DATA_FEED_NAME = os.environ.get("ALPACA_DATA_FEED", "iex").strip().lower()
DATA_FEEDS = {"sip": DataFeed.SIP, "iex": DataFeed.IEX}
if DATA_FEED_NAME not in DATA_FEEDS:
    raise ValueError("ALPACA_DATA_FEED must be 'sip' or 'iex'.")
DATA_FEED = DATA_FEEDS[DATA_FEED_NAME]

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

# Keep paper trading enabled by default, but require an explicit opt-in for live orders.
if not PAPER_MODE and os.environ.get("ALLOW_LIVE_TRADING", "false").lower() != "true":
    ORDERS_ENABLED = False
    print("WARNING: live account selected without ALLOW_LIVE_TRADING=true; order submission disabled.")

DEFAULT_DB_PATH = (
    "/data/trade_history.db"
    if os.path.isdir("/data")
    else "trade_history.db"
)

TRADE_DB_PATH = os.environ.get(
    "TRADE_DB_PATH",
    DEFAULT_DB_PATH
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
print("Timeframe:       1 minute (V3 Giveback)")
print("Entry windows:   ticker-specific V3 config")
print(f"Data feed:       Alpaca {DATA_FEED_NAME.upper()}")
print(f"Contracts:       {CONTRACTS}")
configured_thresholds = sorted({
    threshold
    for ticker in TICKERS
    for threshold in ticker_config(ticker)["thresholds"]
})
print(
    "VWAP thresholds: "
    + ", ".join(f"{threshold * 100:.2f}%" for threshold in configured_thresholds)
    + " (ticker-specific)"
)
print(
    f"Orders:          "
    f"{'ENABLED' if ORDERS_ENABLED else 'DISABLED'}"
)
print(
    f"Alpaca paper:    "
    f"{'YES' if PAPER_MODE else 'NO'}"
)
print(f"Trade DB:         {TRADE_DB_PATH}")
print("========================================")
print()


# ============================================================
# TELEGRAM
# ============================================================

# Telegram sends run on ONE background thread. send_message() uses blocking HTTP with a long
# timeout; calling it directly from the event loop would freeze market data whenever Telegram is slow.
_notify_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="telegram-notify")


def _deliver_notification(message):
    try:
        result = send_message(message)

        if asyncio.iscoroutine(result):
            asyncio.run(result)

    except Exception as exc:
        print(
            f"Telegram notification failed: {exc}"
        )


def notify(message):
    if send_message is None:
        return

    try:
        _notify_executor.submit(_deliver_notification, message)

    except Exception as exc:
        print(
            f"Telegram notification failed: {exc}"
        )


# ============================================================
# DATABASE
# ============================================================

def init_trade_database():

    db_dir = os.path.dirname(os.path.abspath(TRADE_DB_PATH))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(TRADE_DB_PATH)
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            signal_time TEXT NOT NULL,
            signal_close REAL NOT NULL,
            signal_vwap REAL NOT NULL,
            signal_ema9 REAL NOT NULL,
            distance_pct REAL NOT NULL,
            created_at TEXT NOT NULL,
            atr14 REAL,
            threshold REAL,
            UNIQUE(ticker, signal_time, threshold)
        )
        """
    )

    # Backward-compatible migrations for V3 Giveback management state.
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(trades)").fetchall()}
    for name, sql_type in [("atr14", "REAL"), ("peak_underlying", "REAL"),
                           ("giveback_active", "INTEGER"), ("strategy_threshold", "REAL")]:
        if name not in existing:
            cursor.execute(f"ALTER TABLE trades ADD COLUMN {name} {sql_type}")
    pending_cols = {row[1] for row in cursor.execute("PRAGMA table_info(pending_signals)").fetchall()}
    for name, sql_type in [("atr14", "REAL"), ("threshold", "REAL")]:
        if name not in pending_cols:
            cursor.execute(f"ALTER TABLE pending_signals ADD COLUMN {name} {sql_type}")
    # Migrate legacy pending table to allow separate threshold variants on the same candle.
    table_sql_row = cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='pending_signals'").fetchone()
    table_sql = (table_sql_row[0] or "") if table_sql_row else ""
    normalized_table_sql = re.sub(r"\s+", "", table_sql).lower()
    if "unique(ticker,signal_time)" in normalized_table_sql and "unique(ticker,signal_time,threshold)" not in normalized_table_sql:
        cursor.execute("ALTER TABLE pending_signals RENAME TO pending_signals_legacy")
        cursor.execute("""CREATE TABLE pending_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, signal_time TEXT NOT NULL,
            signal_close REAL NOT NULL, signal_vwap REAL NOT NULL, signal_ema9 REAL NOT NULL,
            distance_pct REAL NOT NULL, created_at TEXT NOT NULL, atr14 REAL, threshold REAL,
            UNIQUE(ticker, signal_time, threshold))""")
        legacy_cols = {row[1] for row in cursor.execute("PRAGMA table_info(pending_signals_legacy)").fetchall()}
        atr_expr = "atr14" if "atr14" in legacy_cols else "NULL"
        threshold_expr = "threshold" if "threshold" in legacy_cols else "NULL"
        cursor.execute(f"""INSERT OR IGNORE INTO pending_signals
            (id,ticker,signal_time,signal_close,signal_vwap,signal_ema9,distance_pct,created_at,atr14,threshold)
            SELECT id,ticker,signal_time,signal_close,signal_vwap,signal_ema9,distance_pct,created_at,{atr_expr},{threshold_expr}
            FROM pending_signals_legacy""")
        cursor.execute("DROP TABLE pending_signals_legacy")
    conn.commit()
    conn.close()


def persist_pending_signal(ticker, pending):
    conn = sqlite3.connect(TRADE_DB_PATH)
    conn.execute(
        """INSERT OR IGNORE INTO pending_signals
        (ticker, signal_time, signal_close, signal_vwap, signal_ema9, distance_pct, created_at, atr14, threshold)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, str(pending["signal_time"]), pending["signal_close"],
         pending["signal_vwap"], pending["signal_ema9"], pending["distance_pct"],
         datetime.now(TIMEZONE).isoformat(), pending.get("atr14"), pending.get("threshold")),
    )
    conn.commit()
    conn.close()


def remove_pending_signal(ticker, signal_time, threshold=None):
    conn = sqlite3.connect(TRADE_DB_PATH)
    if threshold is None:
        conn.execute("DELETE FROM pending_signals WHERE ticker = ? AND signal_time = ?", (ticker, str(signal_time)))
    else:
        conn.execute("DELETE FROM pending_signals WHERE ticker = ? AND signal_time = ? AND threshold = ?", (ticker, str(signal_time), float(threshold)))
    conn.commit()
    conn.close()


def load_persistent_pending_signals():
    conn = sqlite3.connect(TRADE_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM pending_signals ORDER BY id").fetchall()
    conn.close()
    return [{
        "ticker": row["ticker"], "signal_time": pd.Timestamp(row["signal_time"]),
        "signal_close": float(row["signal_close"]), "signal_vwap": float(row["signal_vwap"]),
        "signal_ema9": float(row["signal_ema9"]), "distance_pct": float(row["distance_pct"]),
        "atr14": float(row["atr14"]) if "atr14" in row.keys() and row["atr14"] is not None else None,
        "threshold": float(row["threshold"]) if "threshold" in row.keys() and row["threshold"] is not None else None,
    } for row in rows]


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
            created_at,
            atr14, peak_underlying, giveback_active, strategy_threshold
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            entry.get("atr14"), entry.get("peak_underlying", entry.get("entry_underlying")),
            int(bool(entry.get("giveback_active", False))), entry.get("strategy_threshold"),
        ),
    )

    entry["db_id"] = cursor.lastrowid

    conn.commit()
    conn.close()


def save_trade_exit(entry):

    # Keep the in-memory record consistent with the database (Telegram reads this status).
    entry["status"] = "CLOSED"

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
# ALPACA TRADE HISTORY IMPORT
#
# The original bot did not have a persistent local database
# on Railway, so the authoritative historical record is Alpaca.
# On startup we import filled option BUY orders for this bot's
# managed 65-symbol universe into SQLite, then pair filled SELL orders
# against those imported BUYs using FIFO.
#
# Strategy-specific fields that Alpaca does not know (VWAP,
# EMA9, signal candle, etc.) are intentionally left blank.
# ============================================================

def _order_time(order):
    value = (
        getattr(order, "filled_at", None)
        or getattr(order, "submitted_at", None)
        or getattr(order, "created_at", None)
    )
    if value is None:
        return None
    try:
        return pd.Timestamp(value)
    except Exception:
        return None


OCC_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


def _option_details_from_symbol(symbol):
    """Parse an OCC option symbol (Alpaca format, ticker NOT padded).

    Example: AAPL261016C00250000
    """
    match = OCC_RE.match(str(symbol or "").strip())
    if not match:
        return None

    underlying, yymmdd, cp, strike_raw = match.groups()

    if underlying not in MANAGED_TICKERS:
        return None

    try:
        expiration = datetime.strptime(
            yymmdd,
            "%y%m%d",
        ).date()
    except Exception:
        return None

    return {
        "ticker": underlying,
        "option_type": "CALL" if cp == "C" else "PUT",
        "expiration": expiration,
        "strike": int(strike_raw) / 1000.0,
    }


def _get_all_option_orders():
    """Fetch all account option orders.

    NOTE: we do NOT filter by "symbols" here. Option orders are listed
    under the option contract symbol (e.g. AAPL261016C00250000), not the
    underlying ticker, so filtering by TICKERS returned nothing.
    The underlying filter is applied later by _option_details_from_symbol().
    """
    all_orders = []
    after = None

    while True:
        request_kwargs = {
            "status": QueryOrderStatus.ALL,
            "limit": 500,
            "direction": "asc",
            "nested": False,
            "asset_class": AssetClass.US_OPTION,
        }

        if after is not None:
            request_kwargs["after"] = after

        try:
            request = GetOrdersRequest(**request_kwargs)
            batch = trading_client.get_orders(filter=request) or []
        except Exception as exc:
            print(f"Alpaca order-history import failed: {exc}")
            return all_orders

        if not batch:
            break

        all_orders.extend(batch)

        if len(batch) < 500:
            break

        last_time = _order_time(batch[-1])
        if last_time is None:
            break

        # Move the next request just beyond the last submitted timestamp.
        after = last_time.to_pydatetime() + timedelta(microseconds=1)

    # De-duplicate because pagination can overlap at timestamp boundaries.
    unique = {}
    for order in all_orders:
        unique[str(order.id)] = order

    return sorted(
        unique.values(),
        key=lambda o: str(
            getattr(o, "filled_at", None)
            or getattr(o, "submitted_at", None)
            or getattr(o, "created_at", None)
        ),
    )


def _db_entry_order_ids():
    conn = sqlite3.connect(TRADE_DB_PATH)
    rows = conn.execute(
        "SELECT entry_order_id FROM trades WHERE entry_order_id IS NOT NULL"
    ).fetchall()
    conn.close()
    return {str(row[0]) for row in rows if row[0]}


def _insert_imported_entry(order, details):
    fill_qty = getattr(order, "filled_qty", None)
    fill_price = getattr(order, "filled_avg_price", None)
    order_time = _order_time(order)

    if fill_qty is None or float(fill_qty) <= 0 or order_time is None:
        return None

    entry = {
        "ticker": details["ticker"],
        "direction": "LONG",
        "option_symbol": str(order.symbol),
        "option_type": details["option_type"],
        "strike": details["strike"],
        "expiration": details["expiration"],
        "contracts": int(float(fill_qty)),
        "signal_time": order_time,
        "entry_time": order_time,
        "entry_underlying": None,
        "entry_option": float(fill_price) if fill_price is not None else None,
        "entry_order_id": str(order.id),
        "signal_close": None,
        "signal_vwap": None,
        "signal_ema9": None,
        "vwap_distance_pct": None,
        "exit_time": None,
        "exit_underlying": None,
        "exit_option": None,
        "exit_order_id": None,
        "exit_reason": None,
        "pnl": None,
    }

    save_trade_entry(entry)
    return entry


def _apply_imported_sell_fifo(imported_entries, sell_orders):
    """Close imported BUY entries against later SELL fills, FIFO."""
    by_symbol = defaultdict(list)
    for entry in imported_entries:
        by_symbol[entry["option_symbol"]].append(entry)

    for symbol in by_symbol:
        by_symbol[symbol].sort(
            key=lambda e: pd.Timestamp(e["entry_time"])
        )

    sells_by_symbol = defaultdict(list)
    for order in sell_orders:
        symbol = str(getattr(order, "symbol", ""))
        if symbol in by_symbol:
            sells_by_symbol[symbol].append(order)

    for symbol in sells_by_symbol:
        sells_by_symbol[symbol].sort(
            key=lambda o: _order_time(o) or pd.Timestamp.min
        )

    for symbol, sells in sells_by_symbol.items():
        queue = by_symbol[symbol]

        for sell in sells:
            remaining_sell = int(float(getattr(sell, "filled_qty", 0) or 0))
            if remaining_sell <= 0:
                continue

            sell_price = (
                float(sell.filled_avg_price)
                if getattr(sell, "filled_avg_price", None) is not None
                else None
            )
            sell_time = _order_time(sell)

            while remaining_sell > 0 and queue:
                entry = queue[0]
                entry_qty = int(entry.get("contracts") or 0)

                if entry_qty <= 0:
                    queue.pop(0)
                    continue

                matched_qty = min(entry_qty, remaining_sell)

                # Full match: close the imported DB trade normally.
                if matched_qty == entry_qty:
                    entry["exit_time"] = sell_time
                    entry["exit_option"] = sell_price
                    entry["exit_order_id"] = str(sell.id)
                    entry["exit_reason"] = "ALPACA_HISTORY_IMPORT"
                    if (
                        entry.get("entry_option") is not None
                        and sell_price is not None
                    ):
                        entry["pnl"] = (
                            sell_price - float(entry["entry_option"])
                        ) * matched_qty * 100
                    save_trade_exit(entry)
                    queue.pop(0)
                else:
                    # Partial sell: close the sold quantity as a separate
                    # historical trade and leave the remainder OPEN.
                    closed = dict(entry)
                    closed["contracts"] = matched_qty
                    closed["db_id"] = None
                    closed["exit_time"] = sell_time
                    closed["exit_option"] = sell_price
                    closed["exit_order_id"] = str(sell.id)
                    closed["exit_reason"] = "ALPACA_HISTORY_IMPORT"
                    if (
                        entry.get("entry_option") is not None
                        and sell_price is not None
                    ):
                        closed["pnl"] = (
                            sell_price - float(entry["entry_option"])
                        ) * matched_qty * 100

                    # Insert the closed split with a unique synthetic entry
                    # order id so it cannot be imported twice.
                    closed["entry_order_id"] = (
                        f"{entry['entry_order_id']}:PARTIAL:{sell.id}"
                    )
                    save_trade_entry(closed)
                    save_trade_exit(closed)

                    entry["contracts"] = entry_qty - matched_qty
                    # The original DB row remains OPEN with its reduced qty.
                    conn = sqlite3.connect(TRADE_DB_PATH)
                    conn.execute(
                        "UPDATE trades SET contracts = ? WHERE id = ?",
                        (entry["contracts"], entry["db_id"]),
                    )
                    conn.commit()
                    conn.close()

                remaining_sell -= matched_qty


def import_alpaca_trade_history():
    """
    Import missing historical CALL option BUY/SELL
    activity from the Alpaca account.
    """
    orders = _get_all_option_orders()
    if not orders:
        print("Alpaca history import: no account orders returned.")
        return 0
    print(f"Alpaca history import: {len(orders)} account orders retrieved; scanning for filled CALL option orders.")
    existing_ids = _db_entry_order_ids()
    imported_entries = []
    sell_orders = []
    buy_count = 0
    sell_count = 0
    skipped_puts = 0
    skipped_existing = 0
    for order in orders:
        status = str(getattr(order, "status", "")).lower()
        if "filled" not in status:
            continue
        symbol = str(getattr(order, "symbol", "") or "")
        details = _option_details_from_symbol(symbol)
        if details is None:
            continue
        if details["option_type"] != "CALL":
            skipped_puts += 1
            continue
        raw_side = getattr(order, "side", "")
        side = str(raw_side).lower()
        if side.endswith(".buy"):
            side = "buy"
        elif side.endswith(".sell"):
            side = "sell"
        if side == "buy":
            buy_count += 1
            order_id = str(order.id)
            if order_id in existing_ids:
                skipped_existing += 1
                continue
            entry = _insert_imported_entry(order, details)
            if entry is not None:
                imported_entries.append(entry)
                existing_ids.add(order_id)
        elif side == "sell":
            sell_count += 1
            sell_orders.append(order)
    print(f"Historical CALL BUY orders found: {buy_count}")
    print(f"Historical CALL SELL orders found: {sell_count}")
    print(f"Historical CALL orders already in DB: {skipped_existing}")
    print(f"Historical PUT orders skipped: {skipped_puts}")
    if imported_entries:
        _apply_imported_sell_fifo(imported_entries, sell_orders)
    print(f"Alpaca history import complete: {len(imported_entries)} new CALL entries imported.")
    return len(imported_entries)


# ============================================================
# PERSISTENCE / ALPACA RECONCILIATION
# ============================================================

def rebuild_active_entries_from_database():
    for ticker in MANAGED_TICKERS:
        active_entries[ticker] = []

    for trade in all_trades:
        if str(trade.get("status", "")).upper() != "OPEN":
            continue
        ticker = trade.get("ticker")
        if ticker not in active_entries:
            continue
        trade["db_id"] = int(trade["id"]) if trade.get("id") is not None else None
        trade["contracts"] = int(float(trade.get("contracts") or 0))
        trade["entry_underlying"] = float(trade["entry_underlying"]) if trade.get("entry_underlying") is not None else None
        trade["entry_option"] = float(trade["entry_option"]) if trade.get("entry_option") is not None else None
        trade["entry_time"] = pd.Timestamp(trade["entry_time"])
        trade["signal_time"] = pd.Timestamp(trade["signal_time"])
        trade["expiration"] = str(trade.get("expiration") or "")
        active_entries[ticker].append(trade)


def _position_map():
    positions = {}
    try:
        for position in trading_client.get_all_positions():
            symbol = str(position.symbol)
            try:
                qty = int(float(position.qty))
            except Exception:
                qty = 0
            positions[symbol] = {
                "qty": qty,
                "avg_entry_price": float(position.avg_entry_price) if getattr(position, "avg_entry_price", None) is not None else None,
            }
    except Exception as exc:
        print(f"Alpaca position reconciliation failed: {exc}")
        return None
    return positions


def _latest_filled_sell(symbol, after_time=None):
    try:
        orders = _get_all_option_orders()
    except Exception:
        return None

    candidates = []
    for order in orders or []:
        if str(getattr(order, "symbol", "")) != symbol:
            continue
        if str(getattr(order, "side", "")).lower() not in ("sell", "orderside.sell"):
            continue
        if "filled" not in str(getattr(order, "status", "")).lower():
            continue
        submitted = getattr(order, "submitted_at", None) or getattr(order, "filled_at", None)
        if after_time is not None and submitted is not None:
            try:
                if pd.Timestamp(submitted) < pd.Timestamp(after_time):
                    continue
            except Exception:
                pass
        candidates.append(order)

    if not candidates:
        return None
    candidates.sort(
        key=lambda o: str(
            getattr(o, "filled_at", None)
            or getattr(o, "submitted_at", None)
        )
    )
    return candidates[-1]


def mark_entry_external_close(entry, current_time, reason="MANUAL_CLOSE"):
    symbol = entry.get("option_symbol")
    order = _latest_filled_sell(symbol, entry.get("entry_time"))
    exit_price = None
    exit_order_id = None
    exit_time = current_time

    if order is not None:
        exit_order_id = str(order.id)
        if getattr(order, "filled_avg_price", None) is not None:
            exit_price = float(order.filled_avg_price)
        if getattr(order, "filled_at", None) is not None:
            exit_time = pd.Timestamp(order.filled_at)

    entry_price = entry.get("entry_option")
    qty = int(entry.get("contracts") or 0)
    pnl = None
    if entry_price is not None and exit_price is not None:
        pnl = (float(exit_price) - float(entry_price)) * qty * 100

    entry["exit_time"] = exit_time
    entry["exit_underlying"] = None
    entry["exit_option"] = exit_price
    entry["exit_order_id"] = exit_order_id
    entry["exit_reason"] = reason
    entry["pnl"] = pnl
    save_trade_exit(entry)

    pnl_text = f"${pnl:+.2f}" if pnl is not None else "N/A"
    notify(
        f"⚠️ POSITION CLOSED OUTSIDE BOT\n"
        f"{entry.get('ticker')} — LONG CALL\n"
        f"Contract: {symbol}\n"
        f"Contracts: {qty}\n"
        f"Exit: {('$'+format(exit_price, '.2f')) if exit_price is not None else 'N/A'}\n"
        f"P&L: {pnl_text}\n"
        f"Reason: {reason}"
    )


def _entry_age_seconds(entry, now):
    """Seconds since the entry was opened; very large if it cannot be determined."""
    try:
        return (pd.Timestamp(now) - pd.Timestamp(entry.get("entry_time"))).total_seconds()
    except Exception:
        return 10 ** 9


def reconcile_alpaca_positions(notify_changes=True):
    # Runs in a worker thread (see reconciliation_loop) while the event loop keeps adding and
    # closing entries, so it works on a snapshot and merges concurrent changes back at the end.
    positions = _position_map()
    if positions is None:
        return False

    now = datetime.now(TIMEZONE)
    changed = False

    for ticker in MANAGED_TICKERS:
        snapshot = list(active_entries[ticker])
        if not snapshot:
            continue

        by_symbol = defaultdict(list)
        for entry in snapshot:
            by_symbol[str(entry.get("option_symbol"))].append(entry)

        new_entries = []
        for symbol, symbol_entries in by_symbol.items():
            # An entry filled seconds ago can be missing from a position list fetched just before
            # the fill. Never treat a brand-new entry as "closed outside the bot".
            if any(_entry_age_seconds(e, now) < RECONCILE_GRACE_SECONDS for e in symbol_entries):
                new_entries.extend(symbol_entries)
                continue

            actual_qty = int(positions.get(symbol, {}).get("qty", 0))
            expected_qty = sum(int(e.get("contracts") or 0) for e in symbol_entries)

            if actual_qty <= 0:
                for entry in symbol_entries:
                    mark_entry_external_close(entry, now, "MANUAL_CLOSE")
                changed = True
                continue

            if actual_qty >= expected_qty:
                new_entries.extend(symbol_entries)
                continue

            # Partial external close: reconcile the newest bot entries first.
            remaining_qty = actual_qty
            for entry in reversed(symbol_entries):
                qty = int(entry.get("contracts") or 0)
                if remaining_qty >= qty:
                    new_entries.insert(0, entry)
                    remaining_qty -= qty
                elif remaining_qty > 0:
                    closed_qty = qty - remaining_qty
                    clone = dict(entry)
                    clone["contracts"] = remaining_qty
                    new_entries.insert(0, clone)
                    closed = dict(entry)
                    closed["contracts"] = closed_qty
                    mark_entry_external_close(closed, now, "MANUAL_PARTIAL_CLOSE")
                    remaining_qty = 0
                    changed = True
                else:
                    mark_entry_external_close(entry, now, "MANUAL_CLOSE")
                    changed = True

        # Keep entries the event loop added while this pass ran, and drop any it closed meanwhile.
        added = [e for e in active_entries[ticker] if not any(e is s for s in snapshot)]
        active_entries[ticker] = [e for e in new_entries if e.get("exit_time") is None] + added

    # Alert (once per contract) about call positions in the account that the bot does not track.
    tracked = {
        str(e.get("option_symbol"))
        for entries in active_entries.values()
        for e in entries
    }
    for symbol, info in positions.items():
        if symbol in tracked or symbol in warned_unknown_positions:
            continue
        details = _option_details_from_symbol(symbol)
        if details is None or details["option_type"] != "CALL" or int(info.get("qty") or 0) <= 0:
            continue
        warned_unknown_positions.add(symbol)
        print(f"UNTRACKED POSITION in Alpaca: {symbol} qty={info.get('qty')}")
        notify(
            f"⚠️ UNTRACKED POSITION\n"
            f"{symbol}\n"
            f"Contracts held at Alpaca: {info.get('qty')}\n"
            f"The bot has no open trade for it, so it will NOT manage or exit it."
        )

    return changed


async def reconciliation_loop():
    while True:
        try:
            await asyncio.to_thread(reconcile_alpaca_positions)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Position reconciliation error: {exc}")
        await asyncio.sleep(30)


# ============================================================
# STATE
# ============================================================

one_minute_history = {ticker: [] for ticker in MANAGED_TICKERS}
active_entries = {ticker: [] for ticker in MANAGED_TICKERS}
pending_entries = {ticker: [] for ticker in MANAGED_TICKERS}
all_trades = []
last_processed_1m = {ticker: None for ticker in MANAGED_TICKERS}
last_processed_open_minute = {ticker: None for ticker in MANAGED_TICKERS}
last_eod_date = {ticker: None for ticker in MANAGED_TICKERS}

# Keep Telegram's legacy 5-minute history key and semantics separate from V3's 1-minute data.
five_minute_history = {ticker: [] for ticker in MANAGED_TICKERS}
first_trade_by_minute = {ticker: {} for ticker in MANAGED_TICKERS}

# Runtime bookkeeping used by Telegram /status, the heartbeat and the watchdog.
latest_indicators = {}      # ticker -> indicators of the latest completed 1m candle (Telegram /now)
rest_open_lookups = set()   # (ticker, minute) pairs with a REST entry-open lookup in flight
background_tasks = set()    # strong references: asyncio keeps only weak references to tasks
warned_unknown_positions = set()
runtime_stats = {
    "started": time.time(),
    "loop_tick": time.time(),   # refreshed by heartbeat_loop(); the watchdog thread reads it
    "bars_stream": 0,
    "bars_rest": 0,
}

# Alpaca documents a 30-symbol websocket cap on the Basic plan. The docs do not say whether minute-bar
# subscriptions count toward it, so the startup log prints what the server answers and
# rest_bar_gapfill_loop() covers any bars the websocket fails to deliver. Trade channels are
# subscribed only for tickers with pending next-minute entries.
MAX_IEX_TRADE_SYMBOLS = 30
trade_subscribed_tickers = set()
market_data_stream = None

# Subscribe/unsubscribe calls run on ONE worker thread, in order.
#
# alpaca-py's subscribe_trades()/unsubscribe_trades() block until the websocket loop has sent the
# message (asyncio.run_coroutine_threadsafe(...).result()). Called from inside a stream handler -
# which runs ON that loop - they wait for themselves and freeze the whole bot: market data,
# Telegram replies and reconciliation all stop while the process stays alive. A worker thread
# keeps the loop free.
_stream_control_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="stream-control")


def _subscribe_trades_worker(ticker):
    try:
        market_data_stream.subscribe_trades(handle_trade, ticker)
        print(f"IEX trade channel subscribed for pending entry: {ticker} ({len(trade_subscribed_tickers)}/{MAX_IEX_TRADE_SYMBOLS})")
    except Exception as exc:
        trade_subscribed_tickers.discard(ticker)
        print(f"Could not subscribe to IEX trades for {ticker}: {exc}")
        notify(f"⚠️ Could not subscribe to IEX trades for {ticker}: {exc}")


def _unsubscribe_trades_worker(ticker):
    try:
        if market_data_stream is not None:
            market_data_stream.unsubscribe_trades(ticker)
        print(f"IEX trade channel released: {ticker} ({len(trade_subscribed_tickers)}/{MAX_IEX_TRADE_SYMBOLS})")
    except Exception as exc:
        print(f"Could not release IEX trade channel for {ticker}: {exc}")


def ensure_trade_subscription(ticker):
    """Subscribe to a ticker's trade channel while it has a pending entry (never blocks the loop)."""
    if DATA_FEED_NAME != "iex" or ticker in trade_subscribed_tickers:
        return ticker in trade_subscribed_tickers
    if market_data_stream is None:
        return False
    if len(trade_subscribed_tickers) >= MAX_IEX_TRADE_SYMBOLS:
        print(f"IEX trade subscription cap ({MAX_IEX_TRADE_SYMBOLS}) reached; cannot subscribe {ticker} for next-open validation.")
        notify(f"⚠️ IEX trade subscription cap reached; {ticker} entry timing may be missed.")
        return False
    trade_subscribed_tickers.add(ticker)  # reserve the slot now; rolled back if the worker fails
    _stream_control_executor.submit(_subscribe_trades_worker, ticker)
    return True


def release_trade_subscription_if_idle(ticker):
    """Release an IEX trade channel once no pending entries need it (never blocks the loop)."""
    if DATA_FEED_NAME != "iex" or ticker not in trade_subscribed_tickers or pending_entries.get(ticker):
        return
    trade_subscribed_tickers.discard(ticker)
    first_trade_by_minute[ticker].clear()
    _stream_control_executor.submit(_unsubscribe_trades_worker, ticker)

def refresh_telegram_five_minute_history(ticker):
    """Maintain completed 5-minute candles for existing Telegram commands only."""
    rows = one_minute_history[ticker]
    if not rows:
        return
    df = pd.DataFrame(rows).sort_values("time")
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time")
    agg = df.resample("5min", origin="start_day").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna(subset=["open", "high", "low", "close"])
    # Only publish buckets whose final minute (xx:x4 or xx:x9) has completed.
    if agg.empty:
        return
    current = pd.Timestamp(rows[-1]["time"])
    completed = agg[agg.index + pd.Timedelta(minutes=4) <= current]
    records = []
    if not completed.empty:
        completed = completed.copy()
        completed["ticker"] = ticker
        completed["ema9"] = completed["open"].ewm(span=EMA_LENGTH, adjust=False, min_periods=EMA_LENGTH).mean()
        tp = (completed["high"] + completed["low"] + completed["close"]) / 3.0
        completed["session_date"] = completed.index.date
        pv = tp * completed["volume"].fillna(0)
        cumulative_volume = completed["volume"].groupby(completed["session_date"]).cumsum()
        completed["session_vwap"] = pv.groupby(completed["session_date"]).cumsum() / cumulative_volume.where(cumulative_volume != 0)
        records = completed.reset_index(names="time").to_dict("records")[-500:]
    five_minute_history[ticker] = records


# Restore persisted pending signals after state containers exist.
def restore_pending_entries_from_database():
    for pending in load_persistent_pending_signals():
        ticker = pending.pop("ticker")
        if ticker not in pending_entries:
            continue
        # Pending signals older than the immediate next 1-minute candle are stale.
        signal_time = pd.Timestamp(pending["signal_time"])
        now = datetime.now(TIMEZONE)
        if signal_time.date() != now.date():
            remove_pending_signal(ticker, signal_time)
            continue
        if (now - signal_time).total_seconds() > 2 * 60:
            remove_pending_signal(ticker, signal_time)
            continue
        if pending.get("threshold") is None:
            cfg = ticker_config(ticker)
            pending["threshold"] = cfg["thresholds"][0]
            conn = sqlite3.connect(TRADE_DB_PATH)
            conn.execute("UPDATE pending_signals SET threshold=? WHERE ticker=? AND signal_time=? AND threshold IS NULL", (pending["threshold"], ticker, str(signal_time)))
            conn.commit(); conn.close()
        pending_entries[ticker].append(pending)


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
        < datetime.strptime(
            "16:00",
            "%H:%M"
        ).time()
    )


# ============================================================
# TODAY'S HISTORICAL 1-MINUTE DATA
# ============================================================

def load_today_history():
    now_ny = datetime.now(TIMEZONE)
    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    end_time = now_ny - timedelta(minutes=1)
    if end_time <= market_open:
        return pd.DataFrame()

    print(f"Loading today's historical 1-minute Alpaca {DATA_FEED_NAME.upper()} bars...")
    # Alpaca's bars limit applies to the entire multi-symbol response, not per symbol.
    # Batch symbols so 65 tickers × ~390 bars cannot silently truncate at 10,000 rows.
    frames = []
    batch_size = 15
    for offset in range(0, len(MANAGED_TICKERS), batch_size):
        symbols = MANAGED_TICKERS[offset:offset + batch_size]
        request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=market_open,
            end=end_time,
            feed=DATA_FEED,
            limit=10000,
        )
        bars = historical_client.get_stock_bars(request)
        part = bars.df.reset_index()
        if part.empty:
            continue
        if "symbol" in part.columns:
            part = part.rename(columns={"symbol": "ticker"})
        frames.append(part)

    if not frames:
        return pd.DataFrame(columns=["ticker", "time", "open", "high", "low", "close", "volume", "vwap"])
    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TIMEZONE)
    df = df.rename(columns={"timestamp": "time"})
    if "vwap" not in df.columns:
        df["vwap"] = (df["high"] + df["low"] + df["close"]) / 3.0
    return df[["ticker", "time", "open", "high", "low", "close", "volume", "vwap"]].sort_values(["ticker", "time"]).reset_index(drop=True)


def add_v3_indicators(df):
    """V3 indicators aligned with the current full-universe backtest notebook."""
    if df.empty:
        return df
    df = df.sort_values("time").copy()
    df["session_date"] = pd.to_datetime(df["time"]).dt.date
    # EMA9 uses OPEN as explicitly required. Reset each session because only today's
    # regular-session bars are loaded into the live process.
    df["ema9"] = df.groupby("session_date")["open"].transform(
        lambda x: x.ewm(span=EMA_LENGTH, adjust=False, min_periods=EMA_LENGTH).mean()
    )
    # Match the V3 backtest notebook: session VWAP uses typical price weighted by volume.
    minute_vwap = (df["high"] + df["low"] + df["close"]) / 3.0
    df["_vwap_volume"] = minute_vwap * df["volume"].fillna(0)
    df["_cum_vwap_volume"] = df.groupby("session_date")["_vwap_volume"].cumsum()
    df["_cum_volume"] = df.groupby("session_date")["volume"].cumsum()
    df["session_vwap"] = (df["_cum_vwap_volume"] / df["_cum_volume"].where(df["_cum_volume"] != 0))
    prev_close = df.groupby("session_date", sort=False)["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"],
         (df["high"] - prev_close).abs(),
         (df["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    # Match the V3 backtest: first bar of each session uses high-low, not prior-session close.
    first_bar = df.groupby("session_date", sort=False).cumcount().eq(0)
    tr.loc[first_bar] = (df.loc[first_bar, "high"] - df.loc[first_bar, "low"])
    df["atr14"] = tr.groupby(df["session_date"]).transform(
        lambda x: x.ewm(alpha=1.0 / ATR_LENGTH, adjust=False, min_periods=ATR_LENGTH).mean()
    )
    return df.drop(columns=["_vwap_volume", "_cum_vwap_volume", "_cum_volume"])


def seed_history(raw_df):
    if raw_df.empty:
        return
    for ticker in TICKERS:
        rows = raw_df[raw_df["ticker"] == ticker].sort_values("time")
        one_minute_history[ticker] = rows.to_dict("records")[-1000:]
        if not rows.empty:
            last_processed_1m[ticker] = pd.Timestamp(rows.iloc[-1]["time"])
            refresh_telegram_five_minute_history(ticker)
            seeded = build_strategy_df(ticker)
            if not seeded.empty:
                record_latest_indicators(ticker, seeded.iloc[-1])


def build_strategy_df(ticker):
    rows = one_minute_history[ticker]
    if len(rows) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("time").drop_duplicates("time", keep="last").reset_index(drop=True)
    if "vwap" not in df.columns:
        df["vwap"] = (df["high"] + df["low"] + df["close"]) / 3.0
    return add_v3_indicators(df)


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

def _status_name(order):
    """'OrderStatus.PARTIALLY_FILLED' or 'partially_filled' -> 'partially_filled'."""
    return str(getattr(order, "status", "") or "").lower().split(".")[-1]


def wait_for_order_fill(
    order_id,
    timeout_seconds=30,
):
    """Poll until the order is filled/canceled/rejected/expired; None if it is still open.

    A partially filled order is NOT treated as complete."""

    start = time.monotonic()

    while time.monotonic() - start < timeout_seconds:

        try:
            order = (
                trading_client
                .get_order_by_id(
                    order_id
                )
            )

            if _status_name(order) in (
                "filled",
                "canceled",
                "cancelled",
                "rejected",
                "expired",
            ):
                return order

        except Exception as exc:

            print(
                "Order status check failed:",
                exc,
            )

        time.sleep(1)

    return None


def cancel_and_read_order(order_id, label):
    """After a confirmation timeout: cancel the order so it cannot fill later as an untracked
    position (entry) or be sold twice (exit), then read its final state."""
    try:
        trading_client.cancel_order_by_id(order_id)
    except Exception as exc:
        print(f"{label}: cancel request failed (the order may already be filled or closed): {exc}")
    time.sleep(1.5)
    try:
        return trading_client.get_order_by_id(order_id)
    except Exception as exc:
        print(f"{label}: could not read the final order state: {exc}")
        return None


def _record_partial_exit(entry, order, sold_qty, current_time, underlying_price, reason):
    """Part of an exit order filled: book the sold slice as its own closed trade and keep the
    remaining contracts open so the next candle can sell them."""
    exit_price = None
    if getattr(order, "filled_avg_price", None):
        exit_price = float(order.filled_avg_price)
    entry_price = entry.get("entry_option")

    closed = dict(entry)
    closed["contracts"] = sold_qty
    closed["db_id"] = None
    closed["entry_order_id"] = f"{entry.get('entry_order_id')}:PARTIAL:{order.id}"
    closed["exit_time"] = current_time
    closed["exit_underlying"] = float(underlying_price)
    closed["exit_option"] = exit_price
    closed["exit_order_id"] = str(order.id)
    closed["exit_reason"] = reason
    closed["pnl"] = (
        (exit_price - float(entry_price)) * sold_qty * 100
        if exit_price is not None and entry_price is not None
        else None
    )
    save_trade_entry(closed)
    save_trade_exit(closed)
    all_trades.append(closed)

    entry["contracts"] = int(entry["contracts"]) - sold_qty
    if entry.get("db_id"):
        try:
            conn = sqlite3.connect(TRADE_DB_PATH)
            conn.execute("UPDATE trades SET contracts = ? WHERE id = ?", (entry["contracts"], entry["db_id"]))
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"Could not persist reduced contract count for {entry.get('ticker')}: {exc}")

    notify(
        f"⚠️ PARTIAL EXIT\n"
        f"{entry.get('ticker')} — LONG CALL\n"
        f"Contract: {entry.get('option_symbol')}\n"
        f"Sold: {sold_qty} | Still open: {entry['contracts']}\n"
        f"Reason: {reason}\n"
        f"The remaining contracts will be sold on the next candle."
    )
    return False

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
    atr14=None,
    threshold=None,
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

    filled_order = wait_for_order_fill(order.id)

    if filled_order is None:
        # Not confirmed in time. Cancel it so it cannot fill later as an untracked position, then
        # look at what actually happened (it may have filled while the cancel was in flight).
        print(f"{ticker}: entry order not confirmed within 30s; cancelling and re-checking.")
        filled_order = cancel_and_read_order(order.id, f"{ticker} entry")

    filled_qty_raw = 0.0
    if filled_order is not None:
        try:
            filled_qty_raw = float(filled_order.filled_qty or 0)
        except Exception:
            filled_qty_raw = 0.0

    if filled_order is None or filled_qty_raw <= 0:
        status = _status_name(filled_order) if filled_order is not None else "unknown"
        print(f"{ticker}: entry order produced no fill (status: {status}).")
        notify(
            f"⚠️ {ticker} CALL entry NOT filled\n"
            f"{option_symbol}\n"
            f"Order status: {status}. No position was recorded."
        )
        return None

    filled_qty = int(filled_qty_raw)
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
        "vwap_distance_pct": float(distance_pct),
        "strategy_threshold": float(threshold) if threshold is not None else None,
        "atr14": float(atr14) if atr14 is not None and pd.notna(atr14) else None,
        "peak_underlying": float(underlying_open),
        "giveback_active": False,
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

def fetch_first_trade_price(ticker, minute):
    """Price of the first trade at or after `minute` - the entry candle's open. None if no trade yet.

    Asked over REST, so it does not depend on the websocket trade channel (which can only see trades
    that happen AFTER it subscribes, i.e. seconds into the entry minute)."""
    start = pd.Timestamp(minute).tz_convert("UTC").to_pydatetime()
    request = StockTradesRequest(
        symbol_or_symbols=ticker,
        start=start,
        end=start + timedelta(minutes=1),
        limit=1,
        feed=DATA_FEED,
    )
    response = historical_client.get_stock_trades(request)
    data = getattr(response, "data", response)
    trades = data.get(ticker) if hasattr(data, "get") else None
    if not trades:
        return None
    return float(trades[0].price)


async def resolve_open_via_rest(ticker, minute):
    """Wait for the first trade of the entry minute, then run the normal next-open entry check."""
    failures = 0
    try:
        deadline = time.monotonic() + ENTRY_OPEN_REST_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if not any(
                pd.Timestamp(p["signal_time"]) + pd.Timedelta(minutes=1) == minute
                for p in pending_entries[ticker]
            ):
                return  # already handled by the websocket path, or expired
            try:
                price = await asyncio.to_thread(fetch_first_trade_price, ticker, minute)
            except Exception as exc:
                failures += 1
                if failures == 1:
                    print(f"{ticker}: REST open lookup failed: {exc}")
                price = None
            if price is not None:
                print(f"{ticker}: REST first trade of {minute:%H:%M} = {price:.4f}")
                execute_pending_open(ticker, minute, price)
                return
            await asyncio.sleep(1.5)
        print(f"{ticker}: no REST trade found for entry minute {minute:%H:%M} within {ENTRY_OPEN_REST_TIMEOUT_SECONDS}s")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"{ticker}: REST open lookup error: {exc}")
    finally:
        rest_open_lookups.discard((ticker, minute))


def schedule_rest_open_lookup(ticker, minute):
    minute = pd.Timestamp(minute)
    key = (ticker, minute)
    if key in rest_open_lookups:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    rest_open_lookups.add(key)
    _spawn(resolve_open_via_rest(ticker, minute))


def queue_valid_signal(ticker, previous, signal):
    # Removed tickers can be streamed to manage existing positions, but never open new V3 trades.
    if ticker not in TICKERS:
        return
    signal_time = pd.Timestamp(signal["time"])
    previous_time = pd.Timestamp(previous["time"])
    if previous_time.date() != signal_time.date():
        return
    cfg = ticker_config(ticker)
    clock = signal_time.time()
    if not (_clock(cfg["start"]) <= clock <= _clock(cfg["end"])):
        return
    if clock >= FINAL_CANDLE_TIME:
        return
    if not (float(previous["close"]) <= float(previous["session_vwap"]) and float(signal["close"]) > float(signal["session_vwap"])):
        return
    close = float(signal["close"]); vwap = float(signal["session_vwap"]); ema_value = signal["ema9"]
    atr = signal.get("atr14")
    # Match the backtest: no entry unless EMA9 and ATR14 are initialized on the signal candle.
    if pd.isna(ema_value) or pd.isna(atr) or float(atr) <= 0 or vwap <= 0:
        return
    ema = float(ema_value)
    if close <= ema:
        return
    distance_pct = (close / vwap - 1.0) * 100.0
    eligible_thresholds = [t for t in cfg["thresholds"] if distance_pct + 1e-12 >= t * 100.0]
    if not eligible_thresholds:
        return
    # Avoid accidental 2x/3x contract exposure when multiple threshold variants
    # qualify on the same candle. Parallel variants can be explicitly enabled.
    selected_thresholds = sorted(eligible_thresholds) if PARALLEL_THRESHOLD_TRADES else [max(eligible_thresholds)]
    for threshold in selected_thresholds:
        threshold_pct = threshold * 100.0
        duplicate = any(pd.Timestamp(p["signal_time"]) == signal_time and abs(float(p.get("threshold", threshold)) - threshold) < 1e-9 for p in pending_entries[ticker])
        if duplicate:
            continue
        pending = {"signal_time": signal_time, "signal_close": close, "signal_vwap": vwap,
                   "signal_ema9": ema, "distance_pct": distance_pct,
                   "atr14": float(atr) if atr is not None and pd.notna(atr) else None,
                   "threshold": threshold}
        pending_entries[ticker].append(pending)
        persist_pending_signal(ticker, pending)
        ensure_trade_subscription(ticker)
        print(f"V3 SIGNAL {ticker} threshold={threshold_pct:.2f}% time={signal_time} close={close:.4f} VWAP={vwap:.4f} EMA9(open)={ema:.4f} ATR14={atr}")
        notify(f"📡 V3 GIVEBACK SIGNAL\n{ticker} | threshold {threshold_pct:.2f}%\nSignal: {signal_time.strftime('%H:%M')} ET\nClose: ${close:.2f} | VWAP: ${vwap:.2f}\nEMA9(open): ${ema:.2f} | ATR14: {float(atr):.4f}")
    # The first next-minute trade can arrive before the completed-bar callback queues this signal.
    next_minute = signal_time + pd.Timedelta(minutes=1)
    cached_open = first_trade_by_minute[ticker].get(next_minute)
    if cached_open is not None:
        execute_pending_open(ticker, next_minute, cached_open)
    elif pending_entries[ticker]:
        # Ask REST for the true first trade of the entry minute (the candle open).
        schedule_rest_open_lookup(ticker, next_minute)


# ============================================================
# NEXT CANDLE OPEN
# ============================================================

def execute_pending_open(ticker, timestamp, open_price):
    if not pending_entries[ticker]:
        return
    minute = pd.Timestamp(timestamp)
    if last_processed_open_minute[ticker] == minute:
        return
    matching = [p for p in pending_entries[ticker] if pd.Timestamp(p["signal_time"]) < minute and minute - pd.Timestamp(p["signal_time"]) == pd.Timedelta(minutes=1)]
    stale = [p for p in pending_entries[ticker] if minute - pd.Timestamp(p["signal_time"]) > pd.Timedelta(minutes=1)]
    for pending in stale:
        remove_pending_signal(ticker, pending["signal_time"], pending.get("threshold"))
    if not matching and not stale:
        return
    last_processed_open_minute[ticker] = minute
    remaining = [p for p in pending_entries[ticker] if p not in matching and p not in stale]
    pending_entries[ticker] = remaining
    for pending in matching:
        remove_pending_signal(ticker, pending["signal_time"], pending.get("threshold"))
        if float(open_price) <= float(pending["signal_ema9"]):
            print(f"{ticker}: V3 entry rejected; next 1m open {open_price:.4f} <= signal EMA9 {pending['signal_ema9']:.4f}")
            continue
        execute_option_entry(
            ticker=ticker, signal_time=pd.Timestamp(pending["signal_time"]), entry_time=minute,
            underlying_open=float(open_price), signal_close=float(pending["signal_close"]),
            signal_vwap=float(pending["signal_vwap"]), signal_ema9=float(pending["signal_ema9"]),
            distance_pct=float(pending["distance_pct"]), atr14=pending.get("atr14"), threshold=pending.get("threshold"),
        )
    release_trade_subscription_if_idle(ticker)


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

    filled_order = wait_for_order_fill(order.id)

    if filled_order is None:
        # Cancel before anything else so the next candle cannot send a second sell for the same contracts.
        print(f"{entry['ticker']}: exit order not confirmed within 30s; cancelling and re-checking.")
        filled_order = cancel_and_read_order(order.id, f"{entry['ticker']} exit")

    sold_raw = 0.0
    if filled_order is not None:
        try:
            sold_raw = float(filled_order.filled_qty or 0)
        except Exception:
            sold_raw = 0.0

    if filled_order is None or sold_raw <= 0:
        status = _status_name(filled_order) if filled_order is not None else "unknown"
        notify(
            f"⚠️ {entry['ticker']} EXIT not filled\n"
            f"{symbol}\n"
            f"Order status: {status}\n"
            f"The position stays open; the bot retries on the next candle."
        )
        return False

    sold_qty = int(sold_raw)
    if sold_qty < qty:
        return _record_partial_exit(entry, filled_order, sold_qty, current_time, underlying_price, reason)
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

def check_v3_exits(ticker, candle, strategy_df):
    """Manage each overlapping option entry independently from the underlying 1m bars."""
    if not active_entries[ticker] or strategy_df.empty:
        return
    current_time = pd.Timestamp(candle["time"]); close = float(candle["close"]); high = float(candle["high"])
    current = strategy_df.iloc[-1]; ema = float(current["ema9"]); atr = current.get("atr14")
    eod = current_time.time() >= FINAL_CANDLE_TIME
    remaining = []
    for entry in list(active_entries[ticker]):
        if entry.get("exit_time") is not None:
            continue  # already closed (for example by reconciliation); drop it
        entry_time = pd.Timestamp(entry["entry_time"])
        if entry_time.date() != current_time.date() or current_time < entry_time:
            remaining.append(entry); continue
        entry_price = entry.get("entry_underlying")
        if entry_price is None:
            # Imported legacy positions may lack the underlying entry price needed for
            # EMA/ATR management; they must still be flattened at the session close.
            if eod:
                if not execute_option_exit(entry, current_time, close, "END_OF_DAY"):
                    remaining.append(entry)
            else:
                remaining.append(entry)
            continue
        entry_price = float(entry_price)
        peak = max(float(entry.get("peak_underlying") or entry_price), high)
        entry["peak_underlying"] = peak
        if entry.get("atr14") is None and atr is not None and pd.notna(atr):
            entry["atr14"] = float(atr)
        entry_atr = entry.get("atr14")
        if entry_atr is not None and float(entry_atr) > 0 and peak >= entry_price + GIVEBACK_ACTIVATION_ATR * float(entry_atr):
            entry["giveback_active"] = True
        # Persist peak/activation so a restart doesn't reset an already-active trail.
        if entry.get("db_id"):
            try:
                conn = sqlite3.connect(TRADE_DB_PATH)
                conn.execute("UPDATE trades SET peak_underlying=?, giveback_active=?, atr14=? WHERE id=?",
                             (peak, int(bool(entry.get("giveback_active"))), entry.get("atr14"), entry["db_id"]))
                conn.commit(); conn.close()
            except Exception as exc:
                print(f"Could not persist giveback state for {ticker}: {exc}")
        reason = None
        if entry.get("giveback_active"):
            denominator = peak - entry_price
            if denominator > 0 and (peak - close) / denominator >= GIVEBACK_FRACTION:
                reason = "GIVEBACK"
        if reason is None and close < ema:
            reason = "EMA9"
        if reason is None and eod:
            reason = "END_OF_DAY"
        if reason is None:
            remaining.append(entry); continue
        if not execute_option_exit(entry, current_time, close, reason):
            remaining.append(entry)
    active_entries[ticker] = remaining


def _num(value):
    """float(value), or None for NaN / missing."""
    try:
        number = float(value)
    except Exception:
        return None
    return None if number != number else number


def record_latest_indicators(ticker, row):
    """Remember the latest completed-candle indicators for Telegram /now."""
    try:
        latest_indicators[ticker] = {
            "time": pd.Timestamp(row["time"]),
            "close": _num(row.get("close")),
            "session_vwap": _num(row.get("session_vwap")),
            "ema9": _num(row.get("ema9")),
            "atr14": _num(row.get("atr14")),
        }
    except Exception as exc:
        print(f"Could not record indicators for {ticker}: {exc}")


def expire_stale_pending(ticker, candle_time):
    """Drop pending entries whose entry minute is over (that minute's own candle has completed)."""
    expired = [
        p for p in pending_entries[ticker]
        if pd.Timestamp(p["signal_time"]) + pd.Timedelta(minutes=1) <= candle_time
    ]
    if not expired:
        return
    for pending in expired:
        remove_pending_signal(ticker, pending["signal_time"], pending.get("threshold"))
        print(f"{ticker}: pending signal {pd.Timestamp(pending['signal_time']):%H:%M} expired - no entry-minute open was available")
    pending_entries[ticker] = [p for p in pending_entries[ticker] if p not in expired]
    release_trade_subscription_if_idle(ticker)


def process_completed_1m_candle(ticker, candle):
    timestamp = pd.Timestamp(candle["time"])
    last = last_processed_1m[ticker]
    # Ignore duplicates and anything not newer than what was already processed: a late REST or
    # stream bar must never be evaluated out of order.
    if last is not None and timestamp <= pd.Timestamp(last):
        return
    last_processed_1m[ticker] = timestamp
    one_minute_history[ticker].append(candle)
    one_minute_history[ticker] = one_minute_history[ticker][-1000:]
    strategy_df = build_strategy_df(ticker)
    if strategy_df.empty:
        return
    record_latest_indicators(ticker, strategy_df.iloc[-1])
    check_v3_exits(ticker, candle, strategy_df)
    expire_stale_pending(ticker, timestamp)
    if timestamp.time() >= FINAL_CANDLE_TIME:
        last_eod_date[ticker] = timestamp.date()
        for pending in list(pending_entries[ticker]):
            remove_pending_signal(ticker, pending["signal_time"])
        pending_entries[ticker] = []
        release_trade_subscription_if_idle(ticker)
        return
    if len(strategy_df) < 2:
        return
    queue_valid_signal(ticker, strategy_df.iloc[-2], strategy_df.iloc[-1])


# ============================================================
# 1-MINUTE BAR HANDLER
# ============================================================

def guarded(handler):
    """Keep one failing callback from aborting the rest of a websocket message batch.

    alpaca-py dispatches every message of a frame in sequence; an exception escaping a handler
    would skip the remaining tickers' bars for that minute."""
    @functools.wraps(handler)
    async def wrapper(arg):
        try:
            await handler(arg)
        except Exception as exc:
            print(f"HANDLER ERROR in {handler.__name__}: {exc!r}")
            traceback.print_exc()
    return wrapper


@guarded
async def handle_bar(bar):
    ticker = bar.symbol
    if ticker not in one_minute_history:
        return
    timestamp = ny_time(bar.timestamp)
    if not is_regular_session_time(timestamp):
        return
    runtime_stats["bars_stream"] += 1
    record = {"ticker": ticker, "time": timestamp, "open": float(bar.open), "high": float(bar.high),
              "low": float(bar.low), "close": float(bar.close), "volume": float(bar.volume),
              "vwap": float(getattr(bar, "vwap", 0) or 0)}
    # Entry timing is handled by the live trade stream at the first trade of the next minute.
    # Keep a complete intraday session to calculate indicators without losing session VWAP/EMA.
    existing = one_minute_history[ticker]
    if existing and pd.Timestamp(existing[-1]["time"]) == timestamp:
        existing[-1] = record
        return
    process_completed_1m_candle(ticker, record)
    # Telegram's legacy 5-minute view only changes when a 5-minute bucket completes.
    if timestamp.minute % 5 == 4:
        refresh_telegram_five_minute_history(ticker)

@guarded
async def handle_trade(trade):
    ticker = getattr(trade, "symbol", None)
    if ticker not in pending_entries:
        return
    timestamp = ny_time(trade.timestamp)
    if not is_regular_session_time(timestamp):
        return
    minute = timestamp.floor("min")
    # Store only the first observed trade in each minute; this is the earliest live proxy
    # for the next candle open and prevents the completed bar from causing a late entry.
    bucket_map = first_trade_by_minute[ticker]
    if minute not in bucket_map:
        bucket_map[minute] = float(trade.price)
    # Keep memory bounded to the latest 10 minute buckets per ticker.
    for old_minute in sorted(bucket_map)[:-10]:
        bucket_map.pop(old_minute, None)
    execute_pending_open(ticker, minute, bucket_map[minute])


# ============================================================
# BACKGROUND SAFETY TASKS
# ============================================================

def _spawn(coro):
    """Run a coroutine as a background task and keep a strong reference to it."""
    task = asyncio.get_running_loop().create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


def market_is_open_now(now=None):
    now = now or datetime.now(TIMEZONE)
    return now.weekday() < 5 and _clock("09:30") <= now.time() < _clock("16:00")


def last_bar_time():
    stamps = [pd.Timestamp(last_processed_1m[t]) for t in TICKERS if last_processed_1m[t] is not None]
    return max(stamps) if stamps else None


def strategy_description():
    """Static strategy facts for Telegram, built from the live constants so the text cannot drift."""
    windows = {}
    for ticker in TICKERS:
        cfg = ticker_config(ticker)
        windows[ticker] = {"start": cfg["start"], "end": cfg["end"],
                           "thresholds": list(cfg["thresholds"]), "source": cfg["source"]}
    return {
        "name": "V3 Giveback",
        "timeframe": "1 minute",
        "contracts": CONTRACTS,
        "universe_count": len(TICKERS),
        "removed": list(REMOVED_TICKERS),
        "default_start": DEFAULT_ENTRY_START,
        "default_end": DEFAULT_ENTRY_END,
        "default_thresholds": list(DEFAULT_THRESHOLDS),
        "windows": windows,
        "ema_length": EMA_LENGTH,
        "atr_length": ATR_LENGTH,
        "giveback_activation_atr": GIVEBACK_ACTIVATION_ATR,
        "giveback_fraction": GIVEBACK_FRACTION,
        "eod_candle": FINAL_CANDLE_TIME.strftime("%H:%M"),
        "eod_flatten_seconds": EOD_FLATTEN_SECONDS_BEFORE_CLOSE,
        "parallel_thresholds": PARALLEL_THRESHOLD_TRADES,
        "feed": DATA_FEED_NAME.upper(),
        "orders_enabled": ORDERS_ENABLED,
        "paper": PAPER_MODE,
    }


def runtime_snapshot():
    """Read-only runtime facts for Telegram /status (safe to call from any thread)."""
    now = datetime.now(TIMEZONE)
    last = last_bar_time()
    return {
        "now": now,
        "market_open": market_is_open_now(now),
        "uptime_seconds": time.time() - runtime_stats["started"],
        "loop_age_seconds": time.time() - runtime_stats["loop_tick"],
        "bars_stream": runtime_stats["bars_stream"],
        "bars_rest": runtime_stats["bars_rest"],
        "last_bar_time": last,
        "last_bar_age_seconds": (pd.Timestamp(now) - last).total_seconds() if last is not None else None,
        "tickers_with_data": sum(1 for t in TICKERS if latest_indicators.get(t)),
        "trade_subscriptions": len(trade_subscribed_tickers),
        "max_trade_subscriptions": MAX_IEX_TRADE_SYMBOLS,
        "pending_signals": sum(len(v) for v in pending_entries.values()),
        "open_positions": sum(len(v) for v in active_entries.values()),
    }


def build_telegram_state():
    """The shared state handed to telegram_bot (the same objects the strategy mutates)."""
    return {
        "active_entries": active_entries,
        "all_trades": all_trades,
        "five_minute_history": five_minute_history,  # legacy 5-minute view, kept for compatibility
        "one_minute_history": one_minute_history,
        "latest_indicators": latest_indicators,
        "pending_entries": pending_entries,
        "tickers": list(TICKERS),
        "strategy": strategy_description(),
        "runtime": runtime_snapshot,
    }


async def heartbeat_loop():
    """Once a minute: prove the loop is alive, report data flow, alert if bars stop arriving."""
    prev_stream = runtime_stats["bars_stream"]
    prev_rest = runtime_stats["bars_rest"]
    quiet_minutes = 0
    alerted = False
    while True:
        await asyncio.sleep(60)
        runtime_stats["loop_tick"] = time.time()
        now = datetime.now(TIMEZONE)
        d_stream = runtime_stats["bars_stream"] - prev_stream
        d_rest = runtime_stats["bars_rest"] - prev_rest
        prev_stream = runtime_stats["bars_stream"]
        prev_rest = runtime_stats["bars_rest"]
        last = last_bar_time()
        last_text = f"{last:%H:%M}" if last is not None else "none"
        print(
            f"HEARTBEAT {now:%H:%M:%S} ET | bars last min: stream={d_stream} rest={d_rest} "
            f"| last bar {last_text} ET | pending {sum(len(v) for v in pending_entries.values())} "
            f"| open {sum(len(v) for v in active_entries.values())} "
            f"| trade subs {len(trade_subscribed_tickers)}"
        )
        in_session = market_is_open_now(now) and now.time() >= _clock("09:36")
        if in_session and d_stream == 0:
            quiet_minutes += 1
        else:
            quiet_minutes = 0
            if d_stream > 0:
                alerted = False
        if quiet_minutes >= 3 and not alerted:
            alerted = True
            if d_rest > 0:
                note = "The REST gap-filler is covering the missing bars."
            else:
                note = "NO market data is arriving at all."
            print(f"WARNING: no websocket 1m bars for {quiet_minutes} minutes during market hours. {note}")
            notify(
                f"⚠️ No websocket 1-minute bars for {quiet_minutes} minutes during market hours.\n"
                f"{note}\n"
                f"Look for 'symbol limit exceeded' in the Railway logs."
            )


def watchdog_thread():
    """Runs OUTSIDE the event loop, so it still works when the loop is frozen."""
    while True:
        time.sleep(30)
        stalled_for = time.time() - runtime_stats["loop_tick"]
        if stalled_for > WATCHDOG_STALL_SECONDS:
            print(f"WATCHDOG: event loop made no progress for {stalled_for:.0f}s; exiting so Railway restarts the bot.")
            notify(f"🔴 WATCHDOG: the bot's event loop was frozen for {stalled_for:.0f}s. Restarting.")
            time.sleep(4)  # let the notification thread send before the process disappears
            os._exit(1)


def fetch_recent_bars(symbol_starts, end):
    """REST 1-minute bars for {ticker: start}. Records have the same shape as the stream handler's."""
    records = []
    tickers = sorted(symbol_starts)
    batch_size = 15  # Alpaca's bar limit applies to the whole multi-symbol response
    for offset in range(0, len(tickers), batch_size):
        batch = tickers[offset:offset + batch_size]
        request = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=min(symbol_starts[t] for t in batch),
            end=end,
            feed=DATA_FEED,
            limit=10000,
        )
        part = historical_client.get_stock_bars(request).df.reset_index()
        if part.empty:
            continue
        if "symbol" in part.columns:
            part = part.rename(columns={"symbol": "ticker"})
        part["timestamp"] = pd.to_datetime(part["timestamp"], utc=True).dt.tz_convert(TIMEZONE)
        if "vwap" not in part.columns:
            part["vwap"] = (part["high"] + part["low"] + part["close"]) / 3.0
        for row in part.sort_values(["ticker", "timestamp"]).to_dict("records"):
            records.append({
                "ticker": row["ticker"], "time": row["timestamp"],
                "open": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "close": float(row["close"]),
                "volume": float(row["volume"]), "vwap": float(row.get("vwap") or 0),
            })
    return records


async def rest_bar_gapfill_loop():
    """Safety net for missing websocket bars. Once a minute, ask REST for the completed bars of any
    ticker that is behind and feed them through the normal candle pipeline. When the websocket is
    healthy nothing is behind except genuinely illiquid tickers (cheap, one request)."""
    errors = 0
    while True:
        now = datetime.now(TIMEZONE)
        await asyncio.sleep(60 - now.second - now.microsecond / 1e6 + GAPFILL_DELAY_SECONDS)
        try:
            now = datetime.now(TIMEZONE)
            if not (now.weekday() < 5 and _clock("09:31") <= now.time() <= _clock("16:01")):
                continue
            expected = pd.Timestamp(now.replace(second=0, microsecond=0)) - pd.Timedelta(minutes=1)
            session_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            behind = {}
            for ticker in sorted(set(TICKERS) | {t for t, e in active_entries.items() if e}):
                last = last_processed_1m.get(ticker)
                if last is not None and pd.Timestamp(last) >= expected:
                    continue
                start = session_open
                if last is not None and pd.Timestamp(last).date() == now.date():
                    start = max(session_open, (pd.Timestamp(last) - pd.Timedelta(minutes=1)).to_pydatetime())
                behind[ticker] = start
            if not behind:
                continue
            records = await asyncio.to_thread(fetch_recent_bars, behind, now)
            fed = 0
            for record in records:
                ticker = record["ticker"]
                timestamp = pd.Timestamp(record["time"])
                if ticker not in last_processed_1m or not is_regular_session_time(timestamp):
                    continue
                last = last_processed_1m[ticker]
                if last is not None and timestamp <= pd.Timestamp(last):
                    continue
                if timestamp > expected:
                    continue  # the current minute is still open
                process_completed_1m_candle(ticker, record)
                runtime_stats["bars_rest"] += 1
                fed += 1
            if fed:
                print(f"REST gap-fill: fed {fed} missing 1m bars ({len(behind)} tickers were behind)")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errors += 1
            if errors <= 3 or errors % 30 == 0:
                print(f"REST gap-fill error: {exc}")


def flatten_open_entries_for_eod():
    """Sell every open bot position just before the close (the strategy's END_OF_DAY exit)."""
    now = pd.Timestamp(datetime.now(TIMEZONE))
    flattened = 0
    for ticker in list(MANAGED_TICKERS):
        entries = list(active_entries.get(ticker) or [])
        if not entries:
            continue
        rows = one_minute_history.get(ticker) or []
        price = float(rows[-1]["close"]) if rows else float(entries[0].get("entry_underlying") or 0.0)
        remaining = []
        for entry in entries:
            if entry.get("exit_time") is not None:
                continue
            if execute_option_exit(entry, now, price, "END_OF_DAY"):
                flattened += 1
            else:
                remaining.append(entry)
        active_entries[ticker] = remaining
    still_open = sum(len(v) for v in active_entries.values())
    print(f"EOD flatten: closed {flattened} position(s); {still_open} still open")
    if still_open:
        notify(f"⚠️ EOD flatten finished with {still_open} position(s) still open. Check Alpaca now.")
    return flattened


async def eod_flatten_loop():
    if EOD_FLATTEN_SECONDS_BEFORE_CLOSE <= 0:
        print("EOD pre-close flatten: DISABLED")
        return
    done_for = None
    while True:
        await asyncio.sleep(1)
        now = datetime.now(TIMEZONE)
        if now.weekday() >= 5 or done_for == now.date():
            continue
        target = now.replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(seconds=EOD_FLATTEN_SECONDS_BEFORE_CLOSE)
        if target <= now < target + timedelta(seconds=90):
            done_for = now.date()
            try:
                flatten_open_entries_for_eod()
            except Exception as exc:
                print(f"EOD flatten error: {exc}")
                notify(f"🔴 EOD flatten failed: {exc}\nCheck open positions at Alpaca now.")


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
        "Strategy: V3 Giveback, 1m\n"
        f"Data feed: Alpaca {DATA_FEED_NAME.upper()}\n"
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

    # Alpaca is the authoritative source for trades that happened before
    # this Railway deployment. Import anything not already in SQLite.
    try:
        imported_count = import_alpaca_trade_history()
        if imported_count:
            all_trades = load_persistent_trades()
            print(
                f"Trades after Alpaca import: {len(all_trades)}"
            )
    except Exception as exc:
        print(f"Alpaca trade-history import failed: {exc}")
        notify(f"⚠️ Alpaca trade-history import failed\n{exc}")

    rebuild_active_entries_from_database()
    restore_pending_entries_from_database()

    print(
        "Recovered OPEN bot positions: "
        + str(sum(len(v) for v in active_entries.values()))
    )

    # Reconcile persisted bot positions against the actual Alpaca account
    # BEFORE the live stream starts. This also catches manual closes and
    # prevents stale database positions from being treated as active.
    try:
        reconcile_alpaca_positions()
    except Exception as exc:
        print(f"Startup Alpaca reconciliation failed: {exc}")
        notify(f"⚠️ Startup position reconciliation failed\n{exc}")

    # Keep the internal state synchronized with Alpaca while the bot runs.
    _spawn(reconciliation_loop())
    print("Alpaca position reconciliation loop started (30s).")

    # --------------------------------------------------------
    # LOAD TODAY'S HISTORY
    # --------------------------------------------------------

    try:

        raw = (
            load_today_history()
        )

        if not raw.empty:

            seed_history(raw)

            print(
                "Strategy history seeded."
            )

            for ticker in TICKERS:

                print(
                    f"{ticker:5s}: "
                    f"{len(one_minute_history[ticker])} "
                    f"1m bars"
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

            _spawn(
                telegram_poll_loop(
                    build_telegram_state()
                )
            )

        except Exception as exc:

            print(
                "Telegram listener failed:"
            )
            print(exc)
    # --------------------------------------------------------
    # LIVE STREAM
    # --------------------------------------------------------

    global market_data_stream
    stream = StockDataStream(
        API_KEY,
        API_SECRET,
        feed=DATA_FEED,
    )
    market_data_stream = stream

    # Subscribe minute bars for the V3 universe, plus any excluded ticker that still has an open
    # position to manage. Trade events are subscribed only for symbols with pending entries.
    stream_tickers = sorted(set(TICKERS) | {t for t, entries in active_entries.items() if entries})
    for ticker in stream_tickers:
        stream.subscribe_bars(handle_bar, ticker)
    print(
        f"Websocket bar subscriptions requested: {len(stream_tickers)} symbols. Alpaca Basic documents a "
        f"30-symbol websocket cap: look for 'subscribed to bars' (accepted) or 'symbol limit exceeded' "
        f"(rejected; the REST gap-filler then supplies the bars)."
    )

    pending_tickers = [ticker for ticker in MANAGED_TICKERS if pending_entries[ticker]]
    for ticker in pending_tickers[:MAX_IEX_TRADE_SYMBOLS]:
        ensure_trade_subscription(ticker)
    if len(pending_tickers) > MAX_IEX_TRADE_SYMBOLS:
        print(f"WARNING: {len(pending_tickers)} tickers have restored pending entries, exceeding IEX's {MAX_IEX_TRADE_SYMBOLS}-symbol trade-channel cap; some next-open checks cannot be streamed.")


    notify_startup()

    print()
    print(
        "========================================"
    )
    print(
        f"LIVE 1-MINUTE ALPACA {DATA_FEED_NAME.upper()} STREAM STARTED"
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
        "Strategy: V3 Giveback; ticker-specific windows"
    )
    print("VWAP thresholds: ticker-specific 0.30% / 0.35% / 0.40%")
    print(f"Parallel threshold trades: {'ENABLED' if PARALLEL_THRESHOLD_TRADES else 'DISABLED (one order per signal candle)'}")
    print(
        "EMA9: OPEN source"
    )
    print(
        "VWAP: session-anchored typical price × volume (matches V3 backtest notebook)"
    )
    print(
        "Entry: NEXT 1m OPEN, only if above signal EMA9"
    )
    print(
        "Exits: ATR14 giveback (1.5x ATR activation, 35% giveback), EMA9, EOD"
    )
    print(
        "EOD: 15:59 one-minute candle (regular session close)"
    )
    print(
        "Overnight: DISABLED"
    )
    print(
        f"EOD pre-close flatten: {EOD_FLATTEN_SECONDS_BEFORE_CLOSE}s before 16:00 ET "
        f"(0 = off); watchdog {WATCHDOG_STALL_SECONDS}s; REST gap-fill + REST entry-open lookup ON"
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

    runtime_stats["loop_tick"] = time.time()
    _spawn(heartbeat_loop())
    _spawn(rest_bar_gapfill_loop())
    _spawn(eod_flatten_loop())
    threading.Thread(target=watchdog_thread, daemon=True, name="watchdog").start()

    await stream._run_forever()

    # _run_forever() only returns when the stream gives up (for example "insufficient subscription").
    # Returning normally would end the process with exit code 0 and Railway would NOT restart it.
    raise RuntimeError("Market-data stream stopped; exiting so the platform restarts the bot")


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
