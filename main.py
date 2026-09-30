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
            UNIQUE(ticker, signal_time)
        )
        """
    )

    conn.commit()
    conn.close()


def persist_pending_signal(ticker, pending):
    conn = sqlite3.connect(TRADE_DB_PATH)
    conn.execute(
        """
        INSERT OR IGNORE INTO pending_signals
        (ticker, signal_time, signal_close, signal_vwap, signal_ema9, distance_pct, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            str(pending["signal_time"]),
            pending["signal_close"],
            pending["signal_vwap"],
            pending["signal_ema9"],
            pending["distance_pct"],
            datetime.now(TIMEZONE).isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def remove_pending_signal(ticker, signal_time):
    conn = sqlite3.connect(TRADE_DB_PATH)
    conn.execute(
        "DELETE FROM pending_signals WHERE ticker = ? AND signal_time = ?",
        (ticker, str(signal_time)),
    )
    conn.commit()
    conn.close()


def load_persistent_pending_signals():
    conn = sqlite3.connect(TRADE_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM pending_signals ORDER BY id"
    ).fetchall()
    conn.close()
    return [
        {
            "ticker": row["ticker"],
            "signal_time": pd.Timestamp(row["signal_time"]),
            "signal_close": float(row["signal_close"]),
            "signal_vwap": float(row["signal_vwap"]),
            "signal_ema9": float(row["signal_ema9"]),
            "distance_pct": float(row["distance_pct"]),
        }
        for row in rows
    ]


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
# ALPACA TRADE HISTORY IMPORT
#
# The original bot did not have a persistent local database
# on Railway, so the authoritative historical record is Alpaca.
# On startup we import filled option BUY orders for this bot's
# 19-symbol universe into SQLite, then pair filled SELL orders
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


def _option_details_from_symbol(symbol):
    """Parse a standard 21-character OCC option symbol."""
    symbol = str(symbol or "")
    if len(symbol) < 15:
        return None

    underlying = symbol[:6].strip()
    if underlying not in TICKERS:
        return None

    # OCC layout: UNDERLYING(6) + YYMMDD(6) + C/P(1) + STRIKE(8)
    option_type_code = symbol[12:13].upper()
    if option_type_code not in ("C", "P"):
        return None

    try:
        expiration = datetime.strptime(
            symbol[6:12],
            "%y%m%d",
        ).date()
        strike = int(symbol[13:21]) / 1000.0
    except Exception:
        return None

    return {
        "ticker": underlying,
        "option_type": "CALL" if option_type_code == "C" else "PUT",
        "expiration": expiration,
        "strike": strike,
    }


def _get_all_option_orders():
    """Fetch all account option orders, newest API versions included."""
    all_orders = []
    after = None

    while True:
        request_kwargs = {
            "status": QueryOrderStatus.ALL,
            "limit": 500,
            "direction": "asc",
            "nested": False,
            "asset_class": AssetClass.US_OPTION,
            "symbols": TICKERS,
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
    """Import missing historical option BUY/SELL activity from Alpaca."""
    orders = _get_all_option_orders()
    if not orders:
        print("Alpaca history import: no option orders returned.")
        return 0

    existing_ids = _db_entry_order_ids()
    imported_entries = []
    sell_orders = []

    for order in orders:
        status = str(getattr(order, "status", "")).lower()
        if "filled" not in status:
            continue

        symbol = str(getattr(order, "symbol", ""))
        details = _option_details_from_symbol(symbol)
        if details is None:
            continue

        # The bot is CALL-only. Do not turn historical PUT orders into bot
        # trades if the account happened to contain unrelated puts.
        if details["option_type"] != "CALL":
            continue

        side = str(getattr(order, "side", "")).lower()
        if side in ("buy", "orderside.buy"):
            order_id = str(order.id)
            if order_id in existing_ids:
                continue
            entry = _insert_imported_entry(order, details)
            if entry is not None:
                imported_entries.append(entry)
                existing_ids.add(order_id)
        elif side in ("sell", "orderside.sell"):
            sell_orders.append(order)

    if imported_entries:
        _apply_imported_sell_fifo(imported_entries, sell_orders)

    print(
        "Alpaca history import: "
        f"{len(imported_entries)} new CALL entries imported."
    )
    return len(imported_entries)


# ============================================================
# PERSISTENCE / ALPACA RECONCILIATION
# ============================================================

def rebuild_active_entries_from_database():
    for ticker in TICKERS:
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

    for ticker in TICKERS:
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


# Restore persisted pending signals after state containers exist.
def restore_pending_entries_from_database():
    for pending in load_persistent_pending_signals():
        ticker = pending.pop("ticker")
        # Pending signals older than one 5-minute bucket are stale.
        signal_time = pd.Timestamp(pending["signal_time"])
        now = datetime.now(TIMEZONE)
        if signal_time.date() != now.date():
            remove_pending_signal(ticker, signal_time)
            continue
        if (now - signal_time).total_seconds() > 5 * 60:
            remove_pending_signal(ticker, signal_time)
            continue
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

    persist_pending_signal(ticker, pending)

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
            remove_pending_signal(ticker, pending["signal_time"])

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
    for pending in pending_entries[ticker]:
        remove_pending_signal(ticker, pending["signal_time"])
    pending_entries[ticker] = []

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
    # CONTINUOUS POSITION RECONCILIATION
    # --------------------------------------------------------

    asyncio.create_task(reconciliation_loop())
    print("Alpaca position reconciliation loop started (30s).")

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
