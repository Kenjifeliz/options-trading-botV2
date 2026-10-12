import os
import re
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

# SIP is the default consolidated feed. IEX is available only as an explicit override.
DATA_FEED_NAME = os.environ.get("ALPACA_DATA_FEED", "sip").strip().lower()
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
print(f"Trade DB:         {TRADE_DB_PATH}")
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


def reconcile_alpaca_positions(notify_changes=True):
    positions = _position_map()
    if positions is None:
        return False

    now = datetime.now(TIMEZONE)
    changed = False

    for ticker in MANAGED_TICKERS:
        entries = active_entries[ticker]
        if not entries:
            continue

        by_symbol = defaultdict(list)
        for entry in entries:
            by_symbol[str(entry.get("option_symbol"))].append(entry)

        new_entries = []
        for symbol, symbol_entries in by_symbol.items():
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

        active_entries[ticker] = new_entries

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
        print(f"V3 SIGNAL {ticker} threshold={threshold_pct:.2f}% time={signal_time} close={close:.4f} VWAP={vwap:.4f} EMA9(open)={ema:.4f} ATR14={atr}")
        notify(f"📡 V3 GIVEBACK SIGNAL\n{ticker} | threshold {threshold_pct:.2f}%\nSignal: {signal_time.strftime('%H:%M')} ET\nClose: ${close:.2f} | VWAP: ${vwap:.2f}\nEMA9(open): ${ema:.2f} | ATR14: {float(atr):.4f}")
    # The first next-minute trade can arrive before the completed-bar callback queues this signal.
    next_minute = signal_time + pd.Timedelta(minutes=1)
    cached_open = first_trade_by_minute[ticker].get(next_minute)
    if cached_open is not None:
        execute_pending_open(ticker, next_minute, cached_open)


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

def check_v3_exits(ticker, candle, strategy_df):
    """Manage each overlapping option entry independently from the underlying 1m bars."""
    if not active_entries[ticker] or strategy_df.empty:
        return
    current_time = pd.Timestamp(candle["time"]); close = float(candle["close"]); high = float(candle["high"])
    current = strategy_df.iloc[-1]; ema = float(current["ema9"]); atr = current.get("atr14")
    eod = current_time.time() >= FINAL_CANDLE_TIME
    remaining = []
    for entry in list(active_entries[ticker]):
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


def process_completed_1m_candle(ticker, candle):
    timestamp = pd.Timestamp(candle["time"])
    if last_processed_1m[ticker] == timestamp:
        return
    last_processed_1m[ticker] = timestamp
    one_minute_history[ticker].append(candle)
    one_minute_history[ticker] = one_minute_history[ticker][-1000:]
    strategy_df = build_strategy_df(ticker)
    if strategy_df.empty:
        return
    check_v3_exits(ticker, candle, strategy_df)
    if timestamp.time() >= FINAL_CANDLE_TIME:
        last_eod_date[ticker] = timestamp.date()
        for pending in list(pending_entries[ticker]):
            remove_pending_signal(ticker, pending["signal_time"])
        pending_entries[ticker] = []
        return
    if len(strategy_df) < 2:
        return
    queue_valid_signal(ticker, strategy_df.iloc[-2], strategy_df.iloc[-1])


# ============================================================
# 1-MINUTE BAR HANDLER
# ============================================================

async def handle_bar(bar):
    ticker = bar.symbol
    if ticker not in one_minute_history:
        return
    timestamp = ny_time(bar.timestamp)
    if not is_regular_session_time(timestamp):
        return
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
    refresh_telegram_five_minute_history(ticker)


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
    asyncio.create_task(reconciliation_loop())
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
        feed=DATA_FEED,
    )

    for ticker in MANAGED_TICKERS:

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
