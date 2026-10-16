import os
import json
import asyncio
import functools
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# TELEGRAM SETTINGS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

TIMEZONE = ZoneInfo("America/New_York")

# main.py passes its own live universe in state["tickers"]. This list is only a fallback so the
# commands still work if that key is missing; it matches the V3 active universe (51 tickers).
FALLBACK_TICKERS = [
    "AAL", "AAPL", "AMD", "AMZN", "ASTS", "AUR", "BABA", "BAC",
    "BE", "CCL", "COIN", "CSCO", "ET", "F", "GME", "GOOG",
    "GOOGL", "GRAB", "HIMS", "HOOD", "HPE", "INTC", "IREN", "JD",
    "JPM", "LI", "MARA", "META", "MRVL", "MSFT", "MSTR", "MU",
    "NBIS", "NIO", "NOK", "NVDA", "ORCL", "PFE", "PLTR", "QQQ",
    "SNAP", "SOFI", "SOUN", "SPCX", "SPY", "T", "TSLA", "TSM",
    "UAL", "VALE", "XPEV",
]

# Telegram rejects messages over 4096 characters; stay below that.
MAX_MESSAGE_CHARS = 3900

ALPACA_PAPER = (
    os.environ.get("ALPACA_PAPER", "true").lower() == "true"
)

ALPACA_API_KEY = os.environ.get("APCA_API_KEY_ID", "")
ALPACA_SECRET_KEY = os.environ.get("APCA_API_SECRET_KEY", "")

_trading_client = None

# Telegram I/O (long polling, sends, Alpaca account lookups for commands) runs on its own small
# thread pool so it never competes with the bot's Alpaca REST work or blocks the event loop.
_telegram_executor = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="telegram-io",
)


async def _run_blocking(function, *args):
    loop = asyncio.get_running_loop()

    return await loop.run_in_executor(
        _telegram_executor,
        functools.partial(function, *args),
    )


# ============================================================
# ALPACA CLIENT
# ============================================================

def get_trading_client():
    global _trading_client

    if _trading_client is not None:
        return _trading_client

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        return None

    try:
        from alpaca.trading.client import TradingClient

        _trading_client = TradingClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            paper=ALPACA_PAPER,
        )

        return _trading_client

    except Exception as error:
        print(
            f"Telegram Alpaca client error: {error}",
            flush=True,
        )

        return None


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_request(method, data=None):

    if not TELEGRAM_BOT_TOKEN:
        return None

    url = (
        "https://api.telegram.org/bot"
        + TELEGRAM_BOT_TOKEN
        + "/"
        + method
    )

    try:

        if data is None:

            request = urllib.request.Request(
                url
            )

        else:

            encoded = urllib.parse.urlencode(
                data
            ).encode("utf-8")

            request = urllib.request.Request(
                url,
                data=encoded,
            )

        with urllib.request.urlopen(
            request,
            timeout=35,
        ) as response:

            return json.loads(
                response.read().decode("utf-8")
            )

    except Exception as error:

        print(
            f"Telegram API error ({method}): {error}",
            flush=True,
        )

        if "409" in str(error):
            print(
                "Telegram 409 Conflict: another process is polling this bot token "
                "(an old Railway deployment or a copy running elsewhere). "
                "Stop the other instance.",
                flush=True,
            )

        return None


def split_message(text, limit=MAX_MESSAGE_CHARS):
    """Split long text on line boundaries so every part fits Telegram's size limit."""

    text = str(text)

    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""

    for line in text.split("\n"):

        while len(line) > limit:

            if current:
                chunks.append(current)
                current = ""

            chunks.append(line[:limit])
            line = line[limit:]

        candidate = line if not current else current + "\n" + line

        if len(candidate) > limit:
            chunks.append(current)
            current = line

        else:
            current = candidate

    if current:
        chunks.append(current)

    return chunks


def send_message(text):
    """Send text to the configured chat (long text is split). Returns True if every part was sent."""

    if not TELEGRAM_BOT_TOKEN:
        return False

    if not TELEGRAM_CHAT_ID:
        return False

    delivered = True

    for chunk in split_message(text):

        result = telegram_request(
            "sendMessage",
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
            },
        )

        if not result or not result.get("ok"):
            delivered = False

    return delivered


# ============================================================
# BASIC HELPERS
# ============================================================

def format_number(value, decimals=2):

    try:
        return f"{float(value):,.{decimals}f}"

    except Exception:
        return "N/A"


def format_pct(value, decimals=2):

    try:

        value = float(value)

        if value >= 0:
            return f"+{value:.{decimals}f}%"

        return f"{value:.{decimals}f}%"

    except Exception:
        return "N/A"


def format_money(value):

    try:

        value = float(value)

        if value >= 0:
            return f"+${value:,.2f}"

        return f"-${abs(value):,.2f}"

    except Exception:
        return "N/A"


def format_duration(seconds):

    try:
        seconds = int(seconds)

    except Exception:
        return "N/A"

    if seconds < 90:
        return f"{seconds}s"

    minutes = seconds // 60

    if minutes < 90:
        return f"{minutes}m"

    hours, minutes = divmod(minutes, 60)

    if hours < 48:
        return f"{hours}h {minutes}m"

    return f"{hours // 24}d {hours % 24}h"


def parse_datetime(value):

    if value is None:
        return None

    try:

        dt = datetime.fromisoformat(
            str(value)
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=TIMEZONE
            )

        return dt.astimezone(TIMEZONE)

    except Exception:
        return None


def time_text(value):

    dt = parse_datetime(value)

    if dt is None:
        return "N/A"

    return dt.strftime("%H:%M:%S ET")


def get_tickers(state):

    tickers = state.get("tickers")

    if tickers:
        return list(tickers)

    return list(FALLBACK_TICKERS)


def get_strategy(state):

    strategy = state.get("strategy")

    if isinstance(strategy, dict):
        return strategy

    return {}


def get_runtime(state):

    runtime = state.get("runtime")

    try:

        if callable(runtime):
            return runtime()

        if isinstance(runtime, dict):
            return runtime

    except Exception as error:
        print(
            f"Telegram runtime snapshot error: {error}",
            flush=True,
        )

    return {}


def get_active_entries(state):

    active = state.get(
        "active_entries",
        {},
    )

    return {
        ticker: list(entries or [])
        for ticker, entries in list(active.items())
    }


def get_all_trades(state):

    return list(
        state.get(
            "all_trades",
            [],
        )
    )


def get_latest(state):

    return dict(
        state.get(
            "latest_indicators",
            {},
        )
    )


def count_active_positions(state):

    return sum(
        len(entries)
        for entries in get_active_entries(state).values()
    )


def get_trade_status(trade):
    """CLOSED as soon as an exit time exists, whatever stale status text the record still carries."""

    if parse_datetime(trade.get("exit_time")) is not None:
        return "CLOSED"

    status = trade.get("status")

    if status:
        return str(status).upper()

    return "OPEN"


def get_trade_pnl_dollars(trade):

    value = trade.get("pnl")

    if value is None:
        return None

    try:

        value = float(value)

    except Exception:
        return None

    if value != value:
        return None

    return value


def get_today_trades(state):
    """Trades entered today or closed today."""

    today = datetime.now(
        TIMEZONE
    ).date()

    result = []

    for trade in get_all_trades(state):

        entry_time = parse_datetime(
            trade.get("entry_time")
        )

        exit_time = parse_datetime(
            trade.get("exit_time")
        )

        entered_today = (
            entry_time is not None
            and entry_time.date() == today
        )

        closed_today = (
            exit_time is not None
            and exit_time.date() == today
        )

        if entered_today or closed_today:
            result.append(trade)

    return result


def realized_today(state):
    """Closed trades whose exit happened today: (count, wins, losses, pnl_total)."""

    today = datetime.now(
        TIMEZONE
    ).date()

    count = 0
    wins = 0
    losses = 0
    total = 0.0

    for trade in get_all_trades(state):

        exit_time = parse_datetime(
            trade.get("exit_time")
        )

        if exit_time is None or exit_time.date() != today:
            continue

        pnl = get_trade_pnl_dollars(
            trade
        )

        count += 1

        if pnl is None:
            continue

        total += pnl

        if pnl > 0:
            wins += 1

        elif pnl < 0:
            losses += 1

    return count, wins, losses, total


def get_equity():

    client = get_trading_client()

    if client is None:
        return None

    try:

        return client.get_account().equity

    except Exception:
        return None


def clock_time(text):

    return datetime.strptime(
        text,
        "%H:%M",
    ).time()


# ============================================================
# /HELP
# ============================================================

def build_help(state=None, args=None):

    return (
        "🤖 BOT COMMANDS\n\n"

        "/start — Bot status, mode & portfolio size\n"
        "/status — Connection, data flow & runtime health\n"
        "/positions — Open option positions (giveback state)\n"
        "/trades — Today's trades\n"
        "/p&l — Today's realized option P&L\n"
        "/signals — Today's entries + pending signals\n"
        "/summary — Daily overview + V3 strategy rules\n"
        "/now — Live 1-minute indicators (/now AAPL for one)\n"
        "/tickers — The V3 universe, windows & thresholds\n"
        "/overall — Weekly, monthly & yearly performance\n"
        "/help — Show this command list"
    )


# ============================================================
# /START
# ============================================================

def start_message(state, args=None):

    mode = (
        "PAPER"
        if ALPACA_PAPER
        else "LIVE"
    )

    equity = format_number(
        get_equity()
    )

    strategy = get_strategy(state)

    universe = strategy.get(
        "universe_count",
        len(get_tickers(state)),
    )

    return (
        "🟢 BOT STATUS: ONLINE\n"
        f"MODE: {mode}\n"
        f"STRATEGY: V3 Giveback, 1-minute, {universe} tickers\n"
        f"PORTFOLIO SIZE: ${equity}"
    )


# ============================================================
# /STATUS
# ============================================================

def status_message(state, args=None):

    runtime = get_runtime(state)
    strategy = get_strategy(state)

    mode = (
        "PAPER"
        if ALPACA_PAPER
        else "LIVE"
    )

    alpaca_status = "DISCONNECTED"

    client = get_trading_client()

    if client is not None:

        try:

            client.get_account()

            alpaca_status = "CONNECTED"

        except Exception:

            alpaca_status = "DISCONNECTED"

    now = datetime.now(
        TIMEZONE
    )

    tickers = get_tickers(state)

    lines = [
        "🟢 BOT STATUS: ONLINE",
        "",
        f"Mode: {mode}",
    ]

    if "orders_enabled" in strategy:
        lines.append(
            "Orders: "
            + (
                "ENABLED"
                if strategy["orders_enabled"]
                else "DISABLED"
            )
        )

    lines.append(
        f"Data feed: Alpaca {strategy.get('feed', 'IEX')}"
    )

    lines.append(
        f"Alpaca API: {alpaca_status}"
    )

    lines.append(
        f"Strategy: V3 Giveback, 1-minute, "
        f"{strategy.get('universe_count', len(tickers))} tickers"
    )

    lines.append(
        f"Time: {now.strftime('%H:%M:%S ET')}"
    )

    if runtime:

        market_open = runtime.get("market_open")

        lines.append(
            "Market: "
            + (
                "OPEN"
                if market_open
                else "CLOSED"
            )
        )

        last_bar = runtime.get("last_bar_time")
        age = runtime.get("last_bar_age_seconds")

        if last_bar is not None and age is not None:

            text = (
                f"Last 1m bar: {last_bar.strftime('%H:%M')} ET "
                f"({format_duration(age)} ago)"
            )

            if market_open and age > 180:
                text += "  ⚠️ STALE"

            lines.append(text)

        else:

            lines.append(
                "Last 1m bar: none yet"
            )

        lines.append(
            f"Tickers with data: "
            f"{runtime.get('tickers_with_data', 0)}/{len(tickers)}"
        )

        lines.append(
            f"Bars received: websocket {runtime.get('bars_stream', 0)}"
            f" | REST gap-fill {runtime.get('bars_rest', 0)}"
        )

        lines.append(
            f"Trade-channel subs: "
            f"{runtime.get('trade_subscriptions', 0)}"
            f"/{runtime.get('max_trade_subscriptions', 30)}"
        )

        lines.append(
            f"Pending entries: {runtime.get('pending_signals', 0)}"
        )

        loop_age = runtime.get("loop_age_seconds")

        if loop_age is not None:

            lines.append(
                f"Heartbeat: {format_duration(loop_age)} ago"
            )

        lines.append(
            f"Uptime: {format_duration(runtime.get('uptime_seconds'))}"
        )

    lines.append(
        f"Active positions: {count_active_positions(state)}"
    )

    lines.append(
        f"Trades today: {len(get_today_trades(state))}"
    )

    return "\n".join(lines)


# ============================================================
# /POSITIONS
# ============================================================

def positions_message(state, args=None):

    active = get_active_entries(
        state
    )

    latest = get_latest(
        state
    )

    universe = set(
        get_tickers(state)
    )

    lines = [
        "📊 OPEN POSITIONS",
        "",
    ]

    total = 0

    # Every ticker with an open entry is listed, including excluded tickers that still hold a
    # position from an older bot version.
    for ticker in sorted(active):

        for entry in active[ticker]:

            if entry.get("exit_time") is not None:
                continue

            total += 1

            label = (
                f"🟢 {ticker} — CALL"
                if ticker in universe
                else f"🟡 {ticker} — CALL (legacy / excluded ticker)"
            )

            lines.append(label)

            lines.append(
                f"Option: {entry.get('option_symbol', 'N/A')}"
            )

            lines.append(
                f"Contracts: {entry.get('contracts', 0)}"
            )

            entry_underlying = entry.get(
                "entry_underlying"
            )

            if entry_underlying is not None:

                text = (
                    "Underlying entry: $"
                    + format_number(entry_underlying, 2)
                )

                current = (latest.get(ticker) or {}).get("close")

                try:

                    if current:
                        move = (
                            float(current) / float(entry_underlying)
                            - 1.0
                        ) * 100.0

                        text += (
                            f" → ${format_number(current, 2)} "
                            f"({format_pct(move)})"
                        )

                except Exception:
                    pass

                lines.append(text)

            if entry.get("entry_option") is not None:

                lines.append(
                    "Option entry: $"
                    + format_number(entry.get("entry_option"), 4)
                )

            peak = entry.get("peak_underlying")

            if peak is not None and entry_underlying is not None:

                armed = bool(entry.get("giveback_active"))

                lines.append(
                    f"Peak: ${format_number(peak, 2)} | "
                    f"Giveback: {'ARMED' if armed else 'not armed'}"
                )

            if entry.get("atr14") is not None:

                lines.append(
                    f"ATR14 at signal: {format_number(entry.get('atr14'), 4)}"
                )

            if entry.get("strategy_threshold") is not None:

                try:

                    lines.append(
                        "Threshold: "
                        + f"{float(entry['strategy_threshold']) * 100:.2f}%"
                    )

                except Exception:
                    pass

            lines.append(
                "Entry: " + time_text(entry.get("entry_time"))
            )

            lines.append(
                "Status: OPEN"
            )

            lines.append("")

    if total == 0:

        lines.append(
            "No open option positions."
        )

    lines.append(
        f"Total: {total} positions"
    )

    return "\n".join(lines)


# ============================================================
# /TRADES
# ============================================================

def trades_message(state, args=None):

    trades = get_today_trades(
        state
    )

    shown = trades[-40:]

    lines = [
        "📈 TODAY'S TRADES",
        "",
        f"{len(trades)} trades"
        + (
            f" (showing the last {len(shown)})"
            if len(shown) < len(trades)
            else ""
        ),
        "",
    ]

    wins = 0
    losses = 0
    open_trades = 0

    for trade in trades:

        status = get_trade_status(trade)
        pnl = get_trade_pnl_dollars(trade)

        if status == "OPEN":
            open_trades += 1

        elif pnl is not None:

            if pnl > 0:
                wins += 1

            elif pnl < 0:
                losses += 1

    first_index = len(trades) - len(shown) + 1

    for offset, trade in enumerate(shown):

        status = get_trade_status(trade)
        pnl = get_trade_pnl_dollars(trade)

        lines.append(
            f"{first_index + offset}. {trade.get('ticker', '?')} — CALL "
            f"x{trade.get('contracts', '?')}"
        )

        if trade.get("option_symbol"):
            lines.append(
                f"   {trade.get('option_symbol')}"
            )

        lines.append(
            "   Entry: " + time_text(trade.get("entry_time"))
        )

        if status == "OPEN":

            lines.append(
                "   Status: OPEN"
            )

        else:

            lines.append(
                "   Exit: " + time_text(trade.get("exit_time"))
                + (
                    f" ({trade.get('exit_reason')})"
                    if trade.get("exit_reason")
                    else ""
                )
            )

            lines.append(
                "   P&L: "
                + (
                    format_money(pnl)
                    if pnl is not None
                    else "N/A"
                )
            )

        lines.append("")

    lines.extend(
        [
            f"Wins: {wins}",
            f"Losses: {losses}",
            f"Open: {open_trades}",
        ]
    )

    return "\n".join(lines)


# ============================================================
# /P&L
# ============================================================

def pnl_message(state, args=None):

    closed, wins, losses, realized = realized_today(
        state
    )

    portfolio = format_number(
        get_equity()
    )

    return (
        "💰 TODAY'S P&L\n\n"
        f"Realized option P&L: "
        f"{format_money(realized)}\n"
        f"Closed trades: {closed}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Open positions: {count_active_positions(state)}\n\n"
        f"Portfolio: ${portfolio}"
    )


# ============================================================
# /SIGNALS
# ============================================================

def signals_message(state, args=None):

    trades = [
        trade
        for trade in get_today_trades(state)
        if trade.get("signal_time") is not None
    ]

    pending = state.get(
        "pending_entries",
        {},
    )

    pending_rows = []

    for ticker, rows in list(pending.items()):

        for row in list(rows or []):
            pending_rows.append((ticker, row))

    lines = [
        "📡 SIGNALS",
        "",
    ]

    if pending_rows:

        lines.append(
            "Waiting for the next 1-minute open:"
        )

        for ticker, row in pending_rows:

            threshold = row.get("threshold")

            lines.append(
                f"• {ticker} | signal {time_text(row.get('signal_time'))}"
                + (
                    f" | threshold {float(threshold) * 100:.2f}%"
                    if threshold is not None
                    else ""
                )
            )

            lines.append(
                f"  close ${format_number(row.get('signal_close'))} | "
                f"VWAP ${format_number(row.get('signal_vwap'))} | "
                f"EMA9(open) ${format_number(row.get('signal_ema9'))}"
            )

        lines.append("")

    if not trades:

        lines.append(
            "No entered signals recorded today."
        )

        lines.append(
            "(Signals rejected by the next-open EMA9 check are written to the Railway log only.)"
        )

        return "\n".join(lines)

    lines.append(
        "Entered today:"
    )

    for trade in trades[-10:]:

        threshold = trade.get("strategy_threshold")

        lines.append(
            f"• {trade.get('ticker', '?')} — LONG CALL"
            + (
                f" | threshold {float(threshold) * 100:.2f}%"
                if threshold is not None
                else ""
            )
        )

        lines.append(
            f"  Signal: {time_text(trade.get('signal_time'))} | "
            f"Entry: {time_text(trade.get('entry_time'))} | "
            f"{get_trade_status(trade)}"
        )

    lines.append("")

    lines.append(
        f"Total shown: {min(len(trades), 10)} of {len(trades)}"
    )

    return "\n".join(lines)


# ============================================================
# /SUMMARY
# ============================================================

def describe_thresholds(values):

    return " / ".join(
        f"{float(value) * 100:.2f}%"
        for value in values
    )


def strategy_text(state):

    strategy = get_strategy(state)

    if not strategy:

        return (
            "Strategy\n"
            "• V3 Giveback, 1-minute candles\n"
            "• (details unavailable: main.py did not pass its settings)"
        )

    windows = strategy.get("windows", {})

    custom = [
        ticker
        for ticker, cfg in windows.items()
        if cfg.get("source") == "custom"
    ]

    multi = (
        "every qualifying threshold (parallel positions)"
        if strategy.get("parallel_thresholds")
        else "highest qualifying threshold only (one order per signal)"
    )

    return (
        "Strategy: V3 Giveback (1-minute)\n"
        f"• Universe: {strategy.get('universe_count')} tickers "
        f"({len(custom)} custom windows), "
        f"{len(strategy.get('removed', []))} excluded\n"
        f"• Direction: LONG ONLY, CALL ONLY, "
        f"{strategy.get('contracts')} contracts per trade\n"
        f"• Standard window: {strategy.get('default_start')}–"
        f"{strategy.get('default_end')} ET, thresholds "
        f"{describe_thresholds(strategy.get('default_thresholds', []))}\n"
        "• Signal: previous close ≤ VWAP, close > VWAP and "
        "≥ VWAP×(1+threshold), close > EMA9 of OPEN prices\n"
        "• Entry: next 1-minute open, only if above the signal EMA9\n"
        f"• Several thresholds on one candle: {multi}\n"
        f"• Exits: giveback (peak ≥ entry + {strategy.get('giveback_activation_atr')}×ATR"
        f"{strategy.get('atr_length')}, then "
        f"{float(strategy.get('giveback_fraction', 0)) * 100:.0f}% giveback), "
        "close < EMA9, end of day\n"
        f"• End of day: {strategy.get('eod_candle')} candle; open positions are sold "
        f"{strategy.get('eod_flatten_seconds')}s before 16:00 ET\n"
        f"• Feed: Alpaca {strategy.get('feed')} | "
        f"Orders {'ENABLED' if strategy.get('orders_enabled') else 'DISABLED'} | "
        f"{'PAPER' if strategy.get('paper') else 'LIVE'}"
    )


def summary_message(state, args=None):

    today_trades = get_today_trades(
        state
    )

    closed, wins, losses, realized = realized_today(
        state
    )

    mode = (
        "PAPER"
        if ALPACA_PAPER
        else "LIVE"
    )

    return (
        "📊 TODAY'S SUMMARY\n\n"

        "Bot\n"
        "• Status: 🟢 ONLINE\n"
        f"• Mode: {mode}\n\n"

        "Trading\n"
        f"• Trades today: {len(today_trades)}\n"
        f"• Closed today: {closed}\n"
        f"• Wins: {wins}\n"
        f"• Losses: {losses}\n"
        f"• Open positions: {count_active_positions(state)}\n"
        f"• Realized option P&L: {format_money(realized)}\n\n"

        + strategy_text(state)
    )


# ============================================================
# /TICKERS
# ============================================================

def tickers_message(state, args=None):

    tickers = sorted(
        get_tickers(state)
    )

    strategy = get_strategy(state)
    windows = strategy.get("windows", {})

    lines = [
        f"🎯 V3 UNIVERSE: {len(tickers)} tickers",
        "",
        ", ".join(tickers),
        "",
    ]

    custom = sorted(
        ticker
        for ticker, cfg in windows.items()
        if cfg.get("source") == "custom"
    )

    if custom:

        lines.append(
            "Custom windows (ET):"
        )

        for ticker in custom:

            cfg = windows[ticker]

            lines.append(
                f"• {ticker}: {cfg['start']}–{cfg['end']} | "
                f"{describe_thresholds(cfg['thresholds'])}"
            )

        lines.append("")

    if strategy:

        lines.append(
            f"All other {len(tickers) - len(custom)} tickers: "
            f"{strategy.get('default_start')}–{strategy.get('default_end')} | "
            f"{describe_thresholds(strategy.get('default_thresholds', []))}"
        )

        lines.append("")

        removed = strategy.get("removed", [])

        if removed:

            lines.append(
                f"Excluded from new entries ({len(removed)}): "
                + ", ".join(sorted(removed))
            )

    return "\n".join(lines)


# ============================================================
# /NOW
# ============================================================

def in_window(ticker, windows, now):

    cfg = windows.get(ticker)

    if not cfg:
        return None

    try:

        return (
            clock_time(cfg["start"])
            <= now.time()
            <= clock_time(cfg["end"])
        )

    except Exception:
        return None


def ticker_line(ticker, indicators, windows, now):

    close = indicators.get("close")
    vwap = indicators.get("session_vwap")
    ema9 = indicators.get("ema9")
    atr = indicators.get("atr14")

    text = f"{ticker}: ${format_number(close, 2)}"

    try:

        if close and vwap:
            text += f" | VWAP {format_pct((close / vwap - 1.0) * 100.0)}"

        if close and ema9:
            text += f" | EMA9 {format_pct((close / ema9 - 1.0) * 100.0)}"

        if atr:
            text += f" | ATR {format_number(atr, 3)}"

    except Exception:
        pass

    flag = in_window(ticker, windows, now)

    if flag is True:
        text += " | ● in window"

    elif flag is False:
        text += " | ○ out of window"

    return text


def ticker_detail(ticker, state):

    latest = get_latest(state)
    strategy = get_strategy(state)
    windows = strategy.get("windows", {})
    now = datetime.now(TIMEZONE)

    indicators = latest.get(ticker)

    if not indicators:

        return f"{ticker}: no 1-minute data yet."

    lines = [
        f"📡 {ticker} — 1-minute",
        "",
        ticker_line(ticker, indicators, windows, now),
        f"Last completed bar: {indicators['time'].strftime('%H:%M')} ET",
    ]

    cfg = windows.get(ticker)

    close = indicators.get("close")
    vwap = indicators.get("session_vwap")

    if cfg:

        lines.append(
            f"Entry window: {cfg['start']}–{cfg['end']} ET"
        )

        if vwap:

            for threshold in cfg["thresholds"]:

                lines.append(
                    f"Needs close ≥ ${format_number(vwap * (1 + float(threshold)), 2)} "
                    f"(VWAP +{float(threshold) * 100:.2f}%)"
                )

    ema9 = indicators.get("ema9")

    if ema9 and close:

        lines.append(
            "Close is "
            + ("above" if close > ema9 else "below")
            + f" EMA9 (${format_number(ema9, 2)})"
        )

    return "\n".join(lines)


def now_message(state, args=None):

    if args:

        return ticker_detail(
            args[0].upper(),
            state,
        )

    latest = get_latest(state)
    strategy = get_strategy(state)
    windows = strategy.get("windows", {})
    now = datetime.now(TIMEZONE)

    lines = [
        "📡 CURRENT MARKET WATCH (1-minute)",
        "",
    ]

    found = 0

    for ticker in sorted(get_tickers(state)):

        indicators = latest.get(ticker)

        if not indicators or indicators.get("close") is None:
            continue

        lines.append(
            ticker_line(ticker, indicators, windows, now)
        )

        found += 1

    if found == 0:

        lines.append(
            "No live 1-minute candles yet."
        )

    else:

        newest = max(
            indicators["time"]
            for indicators in latest.values()
            if indicators
        )

        lines.insert(
            1,
            f"{found} tickers | latest bar {newest.strftime('%H:%M')} ET | "
            "● = inside entry window",
        )

    return "\n".join(lines)


# ============================================================
# /OVERALL
# ============================================================

def overall_message(state, args=None):

    trades = get_all_trades(
        state
    )

    now = datetime.now(
        TIMEZONE
    )

    midnight = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    period_starts = {
        "TODAY": midnight,

        "THIS WEEK": midnight - timedelta(
            days=now.weekday()
        ),

        "THIS MONTH": midnight.replace(
            day=1
        ),

        "THIS YEAR": midnight.replace(
            month=1,
            day=1,
        ),
    }

    output = [
        "📊 OVERALL PERFORMANCE",
        "",
    ]

    for name, start_date in period_starts.items():

        filtered = []

        for trade in trades:

            if get_trade_status(trade) == "OPEN":
                continue

            # A closed trade counts in the period in which it was closed.
            closed_at = (
                parse_datetime(trade.get("exit_time"))
                or parse_datetime(trade.get("entry_time"))
            )

            if closed_at is None or closed_at < start_date:
                continue

            filtered.append(
                trade
            )

        wins = 0
        losses = 0
        breakeven = 0
        total_pnl = 0.0

        for trade in filtered:

            pnl = get_trade_pnl_dollars(
                trade
            )

            if pnl is None:
                continue

            total_pnl += pnl

            if pnl > 0:
                wins += 1

            elif pnl < 0:
                losses += 1

            else:
                breakeven += 1

        decided = wins + losses + breakeven

        win_rate = (
            wins / decided * 100
            if decided
            else 0.0
        )

        output.append(
            f"{name}\n"
            f"Trades: {len(filtered)}\n"
            f"Wins: {wins}\n"
            f"Losses: {losses}\n"
            f"Breakeven: {breakeven}\n"
            f"Win rate: {win_rate:.2f}%\n"
            f"Option P&L: "
            f"{format_money(total_pnl)}\n"
        )

    return "\n".join(output)


# ============================================================
# COMMAND HANDLER
# ============================================================

COMMANDS = {
    "/start": start_message,
    "/help": build_help,
    "/status": status_message,
    "/positions": positions_message,
    "/trades": trades_message,
    "/p&l": pnl_message,
    "/pnl": pnl_message,
    "/signals": signals_message,
    "/summary": summary_message,
    "/now": now_message,
    "/tickers": tickers_message,
    "/overall": overall_message,
}


async def handle_command(
    command,
    state,
):
    """Run one command. `command` may include arguments (for example '/now AAPL').

    The builders do blocking work (Alpaca account lookups), so they run on the Telegram
    thread pool and never block the bot's event loop."""

    parts = str(command).strip().split()

    if not parts:
        return None

    name = parts[0].split("@")[0].lower()
    args = parts[1:]

    builder = COMMANDS.get(name)

    if builder is None:

        return (
            "Unknown command.\n\n"
            + build_help()
        )

    return await _run_blocking(
        builder,
        state,
        args,
    )


# ============================================================
# TELEGRAM POLLING
# ============================================================

async def telegram_poll_loop(
    state,
):

    print(
        "========================================",
        flush=True,
    )

    print(
        "TELEGRAM LISTENER STARTING",
        flush=True,
    )

    print(
        "========================================",
        flush=True,
    )

    if not TELEGRAM_BOT_TOKEN:

        print(
            "Telegram disabled: TELEGRAM_BOT_TOKEN is missing.",
            flush=True,
        )

        return

    if not TELEGRAM_CHAT_ID:

        print(
            "Telegram disabled: TELEGRAM_CHAT_ID is missing.",
            flush=True,
        )

        return

    print(
        "Telegram credentials detected.",
        flush=True,
    )

    # --------------------------------------------------------
    # Verify bot credentials
    # --------------------------------------------------------

    me = await _run_blocking(
        telegram_request,
        "getMe",
    )

    if not me or not me.get("ok"):

        print(
            "Telegram ERROR: getMe failed. "
            "Check TELEGRAM_BOT_TOKEN.",
            flush=True,
        )

        return

    bot_username = (
        me.get("result", {})
        .get("username", "unknown")
    )

    print(
        f"Telegram bot verified: @{bot_username}",
        flush=True,
    )

    # --------------------------------------------------------
    # Clear old messages
    # --------------------------------------------------------

    offset = 0

    result = await _run_blocking(
        telegram_request,
        "getUpdates",
        {
            "offset": -1,
            "limit": 1,
            "timeout": 1,
        },
    )

    if result and result.get("ok"):

        updates = result.get(
            "result",
            [],
        )

        if updates:

            offset = (
                updates[-1]["update_id"]
                + 1
            )

    print(
        "Telegram command listener started.",
        flush=True,
    )

    # --------------------------------------------------------
    # Poll forever
    # --------------------------------------------------------

    while True:

        try:

            result = await _run_blocking(
                telegram_request,
                "getUpdates",
                {
                    "offset": offset,
                    "timeout": 25,
                },
            )

            if not result:

                await asyncio.sleep(2)

                continue

            if not result.get("ok"):

                print(
                    "Telegram getUpdates failed: "
                    + str(result),
                    flush=True,
                )

                await asyncio.sleep(5)

                continue

            updates = result.get(
                "result",
                [],
            )

            for update in updates:

                offset = (
                    update["update_id"]
                    + 1
                )

                message = update.get(
                    "message"
                )

                if not message:
                    continue

                chat = message.get(
                    "chat",
                    {},
                )

                chat_id = str(
                    chat.get(
                        "id",
                        "",
                    )
                )

                # Only respond to the configured
                # Telegram chat.
                if (
                    TELEGRAM_CHAT_ID
                    and chat_id
                    != str(TELEGRAM_CHAT_ID)
                ):
                    continue

                text = (
                    message.get(
                        "text",
                        "",
                    )
                    or ""
                ).strip()

                if not text:
                    continue

                if not text.startswith("/"):
                    continue

                print(
                    f"Telegram command received: {text.split()[0].split('@')[0].lower()}",
                    flush=True,
                )

                try:

                    response = await handle_command(
                        text,
                        state,
                    )

                    if response:

                        await _run_blocking(
                            send_message,
                            response,
                        )

                except Exception as error:

                    print(
                        "Telegram command error: "
                        + str(error),
                        flush=True,
                    )

                    await _run_blocking(
                        send_message,
                        "⚠️ Telegram command error:\n"
                        + str(error),
                    )

        except asyncio.CancelledError:

            print(
                "Telegram listener stopped.",
                flush=True,
            )

            raise

        except Exception as error:

            print(
                "Telegram listener error: "
                + str(error),
                flush=True,
            )

            await asyncio.sleep(5)
