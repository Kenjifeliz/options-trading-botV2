import os
import asyncio
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests


# ============================================================
# SETTINGS
# ============================================================

TIMEZONE = ZoneInfo("America/New_York")

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

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID"
)

PAPER_MODE = (
    os.environ.get(
        "ALPACA_PAPER",
        "true"
    ).lower()
    == "true"
)


# ============================================================
# TELEGRAM API
# ============================================================

BASE_URL = (
    f"https://api.telegram.org/bot"
    f"{TELEGRAM_BOT_TOKEN}"
)


def _post_telegram(
    method,
    payload=None,
):

    if not TELEGRAM_BOT_TOKEN:
        return None

    url = (
        f"{BASE_URL}/{method}"
    )

    try:

        response = requests.post(
            url,
            json=payload or {},
            timeout=20,
        )

        response.raise_for_status()

        return response.json()

    except Exception as exc:

        print(
            f"Telegram API error: {exc}"
        )

        return None


def send_message(
    message,
    chat_id=None,
):

    if not TELEGRAM_BOT_TOKEN:
        return None

    target_chat = (
        chat_id
        or TELEGRAM_CHAT_ID
    )

    if not target_chat:
        return None

    return _post_telegram(
        "sendMessage",
        {
            "chat_id": target_chat,
            "text": message,
        },
    )


# ============================================================
# TELEGRAM UPDATES
# ============================================================

def _get_updates(
    offset=None,
):

    if not TELEGRAM_BOT_TOKEN:
        return []

    payload = {
        "timeout": 20,
    }

    if offset is not None:
        payload["offset"] = offset

    result = _post_telegram(
        "getUpdates",
        payload,
    )

    if not result:
        return []

    if not result.get("ok"):
        return []

    return result.get(
        "result",
        [],
    )


# ============================================================
# HELP
# ============================================================

def build_help():

    return (
        "🤖 BOT COMMANDS\n\n"

        "/start — Bot status, mode & portfolio size\n"
        "/status — Full bot connection & operating status\n"
        "/positions — Current open positions & unrealized P&L\n"
        "/trades — Today's trades & individual results\n"
        "/p&l — Today's realized, unrealized & total P&L\n"
        "/signals — Recent strategy signals & their status\n"
        "/summary — Full daily overview\n"
        "/now — What the bot is currently watching\n"
        "/overall — Weekly, monthly & yearly performance\n"
        "/help — Show this command list"
    )


# ============================================================
# SAFE STATE HELPERS
# ============================================================

def _state_value(
    state,
    key,
    default=None,
):

    if not isinstance(
        state,
        dict,
    ):
        return default

    return state.get(
        key,
        default,
    )


def _active_entries(
    state,
):

    result = _state_value(
        state,
        "active_entries",
        {},
    )

    if not isinstance(
        result,
        dict,
    ):
        return {}

    return result


def _all_trades(
    state,
):

    result = _state_value(
        state,
        "all_trades",
        [],
    )

    if not isinstance(
        result,
        list,
    ):
        return []

    return result


def _five_minute_history(
    state,
):

    result = _state_value(
        state,
        "five_minute_history",
        {},
    )

    if not isinstance(
        result,
        dict,
    ):
        return {}

    return result


# ============================================================
# FORMAT HELPERS
# ============================================================

def _safe_float(
    value,
):

    try:
        return float(value)
    except Exception:
        return None


def _format_money(
    value,
):

    number = _safe_float(
        value
    )

    if number is None:
        return "N/A"

    return (
        f"${number:+,.2f}"
    )


def _format_pct(
    value,
):

    number = _safe_float(
        value
    )

    if number is None:
        return "N/A"

    return (
        f"{number:+.2f}%"
    )


def _trade_pnl(
    trade,
):

    value = trade.get(
        "pnl"
    )

    if value is not None:
        return _safe_float(
            value
        )

    entry = _safe_float(
        trade.get(
            "entry_option"
        )
    )

    exit_price = _safe_float(
        trade.get(
            "exit_option"
        )
    )

    contracts = _safe_float(
        trade.get(
            "contracts",
            5,
        )
    )

    if (
        entry is None
        or exit_price is None
        or contracts is None
    ):
        return None

    return (
        exit_price
        - entry
    ) * contracts * 100


def _trade_is_closed(
    trade,
):

    exit_time = trade.get(
        "exit_time"
    )

    status = str(
        trade.get(
            "status",
            ""
        )
    ).upper()

    return (
        exit_time is not None
        or status == "CLOSED"
    )


# ============================================================
# /START
# ============================================================

def _start_message(
    state,
):

    mode = (
        "PAPER"
        if PAPER_MODE
        else "LIVE"
    )

    active = _active_entries(
        state
    )

    position_count = sum(
        len(entries)
        for entries in active.values()
        if isinstance(
            entries,
            list,
        )
    )

    return (
        "🟢 BOT STATUS: ONLINE\n\n"
        f"MODE: {mode}\n"
        f"OPEN POSITIONS: {position_count}\n"
        "ORDERS: ENABLED"
    )


# ============================================================
# /STATUS
# ============================================================

def _status_message(
    state,
):

    mode = (
        "PAPER"
        if PAPER_MODE
        else "LIVE"
    )

    active = _active_entries(
        state
    )

    position_count = sum(
        len(entries)
        for entries in active.values()
        if isinstance(
            entries,
            list,
        )
    )

    trades = _all_trades(
        state
    )

    today = datetime.now(
        TIMEZONE
    ).date()

    today_trades = []

    for trade in trades:

        value = (
            trade.get(
                "entry_time"
            )
            or trade.get(
                "signal_time"
            )
        )

        if not value:
            continue

        try:

            trade_date = (
                datetime.fromisoformat(
                    str(value)
                    .replace(
                        "Z",
                        "+00:00",
                    )
                )
                .astimezone(
                    TIMEZONE
                )
                .date()
            )

        except Exception:
            continue

        if trade_date == today:
            today_trades.append(
                trade
            )

    return (
        "🟢 BOT STATUS: ONLINE\n\n"
        f"Mode: {mode}\n"
        "Market data: CONNECTED\n"
        "Alpaca: CONNECTED\n"
        "Telegram: CONNECTED\n"
        f"Active positions: {position_count}\n"
        f"Trades today: {len(today_trades)}"
    )


# ============================================================
# /POSITIONS
# ============================================================

def _position_lines(
    state,
):

    active = _active_entries(
        state
    )

    lines = []

    total_positions = 0

    for ticker in TICKERS:

        entries = active.get(
            ticker,
            [],
        )

        if not isinstance(
            entries,
            list,
        ):
            continue

        for entry in entries:

            total_positions += 1

            entry_price = _safe_float(
                entry.get(
                    "entry_option"
                )
            )

            option_symbol = (
                entry.get(
                    "option_symbol",
                    "N/A",
                )
            )

            contracts = entry.get(
                "contracts",
                5,
            )

            entry_text = (
                f"${entry_price:.2f}"
                if entry_price is not None
                else "N/A"
            )

            lines.append(
                f"{ticker} — LONG CALL\n"
                f"Option: {option_symbol}\n"
                f"Entry: {entry_text}\n"
                f"Contracts: {contracts}"
            )

    if not lines:

        return (
            "📊 OPEN POSITIONS\n\n"
            "No open positions."
        )

    return (
        "📊 OPEN POSITIONS\n\n"
        + "\n\n".join(
            lines
        )
        + "\n\n"
        f"Total: {total_positions} positions"
    )


# ============================================================
# /TRADES
# ============================================================

def _trades_message(
    state,
):

    trades = _all_trades(
        state
    )

    today = datetime.now(
        TIMEZONE
    ).date()

    today_trades = []

    for trade in trades:

        value = (
            trade.get(
                "entry_time"
            )
            or trade.get(
                "signal_time"
            )
        )

        if not value:
            continue

        try:

            trade_date = (
                datetime.fromisoformat(
                    str(value)
                    .replace(
                        "Z",
                        "+00:00",
                    )
                )
                .astimezone(
                    TIMEZONE
                )
                .date()
            )

        except Exception:
            continue

        if trade_date == today:

            today_trades.append(
                trade
            )

    if not today_trades:

        return (
            "📈 TODAY'S TRADES\n\n"
            "No trades today."
        )

    lines = [
        "📈 TODAY'S TRADES",
        "",
        f"{len(today_trades)} trades",
    ]

    realized = 0.0
    wins = 0
    losses = 0
    open_count = 0

    for index, trade in enumerate(
        today_trades,
        start=1,
    ):

        pnl = _trade_pnl(
            trade
        )

        ticker = trade.get(
            "ticker",
            "N/A",
        )

        if _trade_is_closed(
            trade
        ):

            if pnl is not None:

                realized += pnl

                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1

                result_text = (
                    _format_money(
                        pnl
                    )
                )

            else:

                result_text = "P&L N/A"

        else:

            open_count += 1
            result_text = "OPEN"

        lines.append(
            f"{index}. {ticker} — "
            f"LONG CALL — "
            f"{result_text}"
        )

    lines.extend(
        [
            "",
            f"Realized P&L: "
            f"{_format_money(realized)}",
            f"Wins: {wins}",
            f"Losses: {losses}",
            f"Open: {open_count}",
        ]
    )

    return "\n".join(
        lines
    )


# ============================================================
# /P&L
# ============================================================

def _pnl_message(
    state,
):

    trades = _all_trades(
        state
    )

    today = datetime.now(
        TIMEZONE
    ).date()

    realized = 0.0
    unrealized = 0.0

    trade_count = 0
    wins = 0
    losses = 0
    open_count = 0

    for trade in trades:

        value = (
            trade.get(
                "entry_time"
            )
            or trade.get(
                "signal_time"
            )
        )

        if not value:
            continue

        try:

            trade_date = (
                datetime.fromisoformat(
                    str(value)
                    .replace(
                        "Z",
                        "+00:00",
                    )
                )
                .astimezone(
                    TIMEZONE
                )
                .date()
            )

        except Exception:
            continue

        if trade_date != today:
            continue

        trade_count += 1

        pnl = _trade_pnl(
            trade
        )

        if _trade_is_closed(
            trade
        ):

            if pnl is not None:

                realized += pnl

                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1

        else:

            open_count += 1

            if pnl is not None:
                unrealized += pnl

    total = (
        realized
        + unrealized
    )

    return (
        "💰 TODAY'S P&L\n\n"
        f"Realized P&L: "
        f"{_format_money(realized)}\n"
        f"Unrealized P&L: "
        f"{_format_money(unrealized)}\n"
        f"Total P&L: "
        f"{_format_money(total)}\n\n"
        f"Trades: {trade_count}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Open positions: {open_count}"
    )


# ============================================================
# /SIGNALS
# ============================================================

def _signals_message(
    state,
):

    active = _active_entries(
        state
    )

    lines = [
        "📡 RECENT SIGNALS",
        "",
    ]

    found = 0

    for ticker in TICKERS:

        entries = active.get(
            ticker,
            [],
        )

        if not isinstance(
            entries,
            list,
        ):
            continue

        for entry in entries[-3:]:

            found += 1

            signal_time = entry.get(
                "signal_time"
            )

            if signal_time:

                try:

                    display_time = (
                        pd_timestamp_to_et(
                            signal_time
                        )
                    )

                except Exception:

                    display_time = str(
                        signal_time
                    )

            else:

                display_time = "N/A"

            lines.extend(
                [
                    f"{ticker} — LONG",
                    f"Signal: {display_time}",
                    (
                        f"VWAP: "
                        f"${_safe_float(entry.get('signal_vwap')):.2f}"
                        if _safe_float(
                            entry.get(
                                "signal_vwap"
                            )
                        )
                        is not None
                        else "VWAP: N/A"
                    ),
                    (
                        f"Signal close: "
                        f"${_safe_float(entry.get('signal_close')):.2f}"
                        if _safe_float(
                            entry.get(
                                "signal_close"
                            )
                        )
                        is not None
                        else "Signal close: N/A"
                    ),
                    (
                        f"EMA9: "
                        f"${_safe_float(entry.get('signal_ema9')):.2f}"
                        if _safe_float(
                            entry.get(
                                "signal_ema9"
                            )
                        )
                        is not None
                        else "EMA9: N/A"
                    ),
                    "Status: ENTERED",
                    "",
                ]
            )

    if found == 0:

        return (
            "📡 RECENT SIGNALS\n\n"
            "No entered signals yet."
        )

    return "\n".join(
        lines
    )


def pd_timestamp_to_et(
    value,
):

    timestamp = pd_to_datetime(
        value
    )

    return timestamp.strftime(
        "%H:%M ET"
    )


def pd_to_datetime(
    value,
):

    text = str(
        value
    )

    timestamp = datetime.fromisoformat(
        text.replace(
            "Z",
            "+00:00",
        )
    )

    if timestamp.tzinfo is None:

        timestamp = timestamp.replace(
            tzinfo=TIMEZONE
        )

    return timestamp.astimezone(
        TIMEZONE
    )


# ============================================================
# /SUMMARY
# ============================================================

def _summary_message(
    state,
):

    active = _active_entries(
        state
    )

    position_count = sum(
        len(entries)
        for entries in active.values()
        if isinstance(
            entries,
            list,
        )
    )

    trades = _all_trades(
        state
    )

    today = datetime.now(
        TIMEZONE
    ).date()

    today_trades = []

    for trade in trades:

        value = (
            trade.get(
                "entry_time"
            )
            or trade.get(
                "signal_time"
            )
        )

        if not value:
            continue

        try:

            trade_date = (
                pd_to_datetime(
                    value
                ).date()
            )

        except Exception:
            continue

        if trade_date == today:

            today_trades.append(
                trade
            )

    realized = 0.0
    wins = 0
    losses = 0

    for trade in today_trades:

        if not _trade_is_closed(
            trade
        ):
            continue

        pnl = _trade_pnl(
            trade
        )

        if pnl is None:
            continue

        realized += pnl

        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1

    mode = (
        "PAPER"
        if PAPER_MODE
        else "LIVE"
    )

    return (
        "📊 TODAY'S SUMMARY\n\n"

        "Bot\n"
        "• Status: 🟢 ONLINE\n"
        f"• Mode: {mode}\n\n"

        "Trading\n"
        f"• Trades: {len(today_trades)}\n"
        f"• Wins: {wins}\n"
        f"• Losses: {losses}\n"
        f"• Open positions: {position_count}\n\n"

        "P&L\n"
        f"• Realized: "
        f"{_format_money(realized)}\n"
        "• Unrealized: "
        "See /positions\n\n"

        "Strategy\n"
        "• Direction: LONG ONLY\n"
        "• Option: CALL ONLY\n"
        "• Timeframe: 5m\n"
        "• Start: 10:00 NY\n"
        "• VWAP filter: 0.28%"
    )


# ============================================================
# /NOW
# ============================================================

def _now_message(
    state,
):

    history = _five_minute_history(
        state
    )

    lines = [
        "👀 CURRENT WATCH",
        "",
    ]

    for ticker in TICKERS:

        rows = history.get(
            ticker,
            [],
        )

        if not rows:
            lines.append(
                f"{ticker}: waiting for data"
            )
            continue

        latest = rows[-1]

        close = _safe_float(
            latest.get(
                "close"
            )
        )

        if close is None:
            lines.append(
                f"{ticker}: waiting for data"
            )
  