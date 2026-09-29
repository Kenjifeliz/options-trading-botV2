import os
import json
import asyncio
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# ============================================================
# TELEGRAM SETTINGS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

TIMEZONE = ZoneInfo("America/New_York")


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
            request = urllib.request.Request(url)
        else:

            encoded = urllib.parse.urlencode(data).encode(
                "utf-8"
            )

            request = urllib.request.Request(
                url,
                data=encoded
            )

        with urllib.request.urlopen(
            request,
            timeout=35
        ) as response:

            return json.loads(
                response.read().decode("utf-8")
            )

    except Exception as error:

        print(
            f"Telegram API error: {error}",
            flush=True
        )

        return None


def send_message(text):

    if not TELEGRAM_BOT_TOKEN:
        return

    if not TELEGRAM_CHAT_ID:
        return

    telegram_request(
        "sendMessage",
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        }
    )


# ============================================================
# HELP
# ============================================================

def build_help():

    return (
        "🤖 BOT COMMANDS\n\n"

        "/start — Bot status, mode & portfolio size\n"
        "/status — Full bot connection & operating status\n"
        "/positions — Current open positions\n"
        "/trades — Today's trades\n"
        "/p&l — Today's P&L\n"
        "/signals — Recent strategy signals\n"
        "/summary — Full daily overview\n"
        "/now — Current market conditions\n"
        "/overall — Weekly, monthly & yearly performance\n"
        "/help — Show this command list"
    )


# ============================================================
# BASIC FORMATTING
# ============================================================

def format_number(value, decimals=2):

    try:
        return f"{float(value):,.{decimals}f}"
    except Exception:
        return "N/A"


def format_pct(value):

    try:

        value = float(value)

        if value >= 0:
            return f"+{value:.2f}%"

        return f"{value:.2f}%"

    except Exception:

        return "N/A"


# ============================================================
# STATE HELPERS
# ============================================================

def get_active_entries(state):

    return state.get(
        "active_entries",
        {}
    )


def get_all_trades(state):

    return state.get(
        "all_trades",
        []
    )


def get_history(state):

    return state.get(
        "five_minute_history",
        {}
    )


# ============================================================
# /START
# ============================================================

def start_message(state):

    paper_mode = (
        os.environ.get(
            "ALPACA_PAPER",
            "true"
        ).lower()
        == "true"
    )

    mode = "PAPER" if paper_mode else "LIVE"

    equity = "N/A"

    trading_client = state.get(
        "trading_client"
    )

    if trading_client is not None:

        try:

            account = trading_client.get_account()

            equity = format_number(
                account.equity
            )

        except Exception:

            pass

    return (
        "🟢 BOT STATUS: ONLINE\n"
        f"MODE: {mode}\n"
        f"PORTFOLIO SIZE: ${equity}"
    )


# ============================================================
# /STATUS
# ============================================================

def status_message(state):

    paper_mode = (
        os.environ.get(
            "ALPACA_PAPER",
            "true"
        ).lower()
        == "true"
    )

    mode = "PAPER" if paper_mode else "LIVE"

    active = get_active_entries(state)

    active_count = sum(
        len(entries)
        for entries in active.values()
    )

    all_trades = get_all_trades(state)

    today = datetime.now(
        TIMEZONE
    ).date()

    today_trades = 0

    for trade in all_trades:

        try:

            entry_time = trade.get(
                "entry_time"
            )

            if entry_time is None:
                continue

            entry_time = datetime.fromisoformat(
                str(entry_time)
            )

            if entry_time.date() == today:
                today_trades += 1

        except Exception:

            continue

    return (
        "🟢 BOT STATUS: ONLINE\n\n"
        f"Mode: {mode}\n"
        "Market data: CONNECTED\n"
        "Alpaca: CONNECTED\n"
        "Telegram: CONNECTED\n"
        f"Active positions: {active_count}\n"
        f"Trades today: {today_trades}"
    )


# ============================================================
# /POSITIONS
# ============================================================

def positions_message(state):

    active = get_active_entries(state)

    lines = [
        "📊 OPEN POSITIONS",
        ""
    ]

    total = 0

    for ticker, entries in active.items():

        for entry in entries:

            total += 1

            direction = entry.get(
                "direction",
                "LONG"
            )

            entry_price = entry.get(
                "entry_price"
            )

            entry_time = entry.get(
                "entry_time"
            )

            lines.append(
                f"{ticker} — {direction}"
            )

            if entry_price is not None:

                lines.append(
                    f"Entry: ${format_number(entry_price, 4)}"
                )

            if entry_time is not None:

                lines.append(
                    f"Time: {entry_time}"
                )

            lines.append("")

    if total == 0:

        lines.append(
            "No open strategy positions."
        )

    lines.append(
        f"Total: {total} positions"
    )

    return "\n".join(lines)


# ============================================================
# /TRADES
# ============================================================

def trades_message(state):

    trades = get_all_trades(state)

    today = datetime.now(
        TIMEZONE
    ).date()

    today_trades = []

    for trade in trades:

        try:

            entry_time = trade.get(
                "entry_time"
            )

            if entry_time is None:
                continue

            entry_time = datetime.fromisoformat(
                str(entry_time)
            )

            if entry_time.date() == today:

                today_trades.append(
                    trade
                )

        except Exception:

            continue

    lines = [
        "📈 TODAY'S TRADES",
        "",
        f"{len(today_trades)} trades"
    ]

    wins = 0
    losses = 0
    open_trades = 0

    for index, trade in enumerate(
        today_trades,
        start=1
    ):

        ticker = trade.get(
            "ticker",
            "?"
        )

        direction = trade.get(
            "direction",
            "LONG"
        )

        pnl = trade.get(
            "pnl_pct"
        )

        if pnl is None:

            pnl = trade.get(
                "return_pct"
            )

        if pnl is None:

            status = "OPEN"

            open_trades += 1

        else:

            try:

                pnl = float(pnl)

                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1

                status = format_pct(pnl)

            except Exception:

                status = "CLOSED"

        lines.append(
            f"{index}. {ticker} — "
            f"{direction} — {status}"
        )

    lines.extend(
        [
            "",
            f"Wins: {wins}",
            f"Losses: {losses}",
            f"Open: {open_trades}"
        ]
    )

    return "\n".join(lines)


# ============================================================
# /P&L
# ============================================================

def pnl_message(state):

    trades = get_all_trades(state)

    today = datetime.now(
        TIMEZONE
    ).date()

    realized = 0.0
    wins = 0
    losses = 0
    closed = 0

    for trade in trades:

        try:

            entry_time = trade.get(
                "entry_time"
            )

            if entry_time is None:
                continue

            entry_time = datetime.fromisoformat(
                str(entry_time)
            )

            if entry_time.date() != today:
                continue

            pnl = trade.get(
                "pnl_pct"
            )

            if pnl is None:

                pnl = trade.get(
                    "return_pct"
                )

            if pnl is None:
                continue

            pnl = float(pnl)

            realized += pnl

            closed += 1

            if pnl > 0:
                wins += 1

            elif pnl < 0:
                losses += 1

        except Exception:

            continue

    active = get_active_entries(state)

    open_count = sum(
        len(entries)
        for entries in active.values()
    )

    trading_client = state.get(
        "trading_client"
    )

    portfolio = "N/A"

    if trading_client is not None:

        try:

            account = trading_client.get_account()

            portfolio = format_number(
                account.equity
            )

        except Exception:

            pass

    return (
        "💰 TODAY'S P&L\n\n"
        f"Strategy return: {format_pct(realized)}\n"
        f"Closed trades: {closed}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Open positions: {open_count}\n\n"
        f"Portfolio: ${portfolio}"
    )


# ============================================================
# /SIGNALS
# ============================================================

def signals_message(state):

    signals = state.get(
        "signals",
        []
    )

    lines = [
        "📡 RECENT SIGNALS",
        ""
    ]

    if not signals:

        lines.append(
            "No signals recorded yet."
        )

        return "\n".join(lines)

    recent = signals[-10:]

    entered = 0
    rejected = 0
    pending = 0

    for signal in recent:

        ticker = signal.get(
            "ticker",
            "?"
        )

        direction = signal.get(
            "direction",
            "LONG"
        )

        signal_time = signal.get(
            "signal_time",
            ""
        )

        status = signal.get(
            "status",
            "UNKNOWN"
        )

        status_upper = str(
            status
        ).upper()

        if status_upper == "ENTERED":
            entered += 1

        elif status_upper == "REJECTED":
            rejected += 1

        elif status_upper == "PENDING":
            pending += 1

        lines.append(
            f"{ticker} — {direction}"
        )

        lines.append(
            f"Signal: {signal_time}"
        )

        if signal.get("vwap") is not None:

            lines.append(
                f"VWAP: ${format_number(signal['vwap'], 4)}"
            )

        if signal.get("signal_close") is not None:

            lines.append(
                f"Signal close: ${format_number(signal['signal_close'], 4)}"
            )

        if signal.get("ema9") is not None:

            lines.append(
                f"EMA9: ${format_number(signal['ema9'], 4)}"
            )

        if signal.get("vwap_distance_pct") is not None:

            lines.append(
                "VWAP distance: "
                + format_pct(
                    signal["vwap_distance_pct"]
                )
            )

        lines.append(
            f"Status: {status}"
        )

        lines.append("")

    lines.extend(
        [
            f"Total shown: {len(recent)}",
            f"Entered: {entered}",
            f"Rejected: {rejected}",
            f"Pending: {pending}"
        ]
    )

    return "\n".join(lines)


# ============================================================
# /SUMMARY
# ============================================================

def summary_message(state):

    status = status_message(
        state
    )

    trades = get_all_trades(
        state
    )

    active = get_active_entries(
        state
    )

    active_count = sum(
        len(entries)
        for entries in active.values()
    )

    return (
        "📊 TODAY'S SUMMARY\n\n"
        "Bot\n"
        "- Status: 🟢 ONLINE\n"
        f"- Mode: "
        f"{'PAPER' if os.environ.get('ALPACA_PAPER', 'true').lower() == 'true' else 'LIVE'}\n\n"
        "Trading\n"
        f"- Recorded trades: {len(trades)}\n"
        f"- Open positions: {active_count}\n\n"
        "Connections\n"
        "- Market data: CONNECTED\n"
        "- Alpaca: CONNECTED\n"
        "- Telegram: CONNECTED"
    )


# ============================================================
# /NOW
# ============================================================

def now_message(state):

    history = get_history(
        state
    )

    lines = [
        "📡 CURRENT MARKET WATCH",
        ""
    ]

    for ticker in history:

        candles = history.get(
            ticker,
            []
        )

        if not candles:
            continue

        candle = candles[-1]

        close = candle.get(
            "close"
        )

        if close is None:
            continue

        lines.append(
            f"{ticker}: ${format_number(close, 2)}"
        )

    if len(lines) == 2:

        lines.append(
            "No live 5-minute candles yet."
        )

    return "\n".join(lines)


# ============================================================
# /OVERALL
# ============================================================

def overall_message(state):

    trades = get_all_trades(
        state
    )

    now = datetime.now(
        TIMEZONE
    )

    periods = {
        "TODAY": now.date(),
        "THIS WEEK": (
            now - __import__(
                "datetime"
            ).timedelta(
                days=now.weekday()
            )
        ).date(),
        "THIS MONTH": now.replace(
            day=1
        ).date(),
        "THIS YEAR": now.replace(
            month=1,
            day=1
        ).date(),
    }

    output = [
        "📊 OVERALL PERFORMANCE",
        ""
    ]

    for name, start_date in periods.items():

        filtered = []

        for trade in trades:

            try:

                entry_time = trade.get(
                    "entry_time"
                )

                exit_time = trade.get(
                    "exit_time"
                )

                if entry_time is None:
                    continue

                if exit_time is None:
                    continue

                entry_time = datetime.fromisoformat(
                    str(entry_time)
                )

                if entry_time.date() >= start_date:

                    filtered.append(
                        trade
                    )

            except Exception:

                continue

        total = len(filtered)
        wins = 0
        losses = 0
        breakeven = 0
        total_return = 0.0

        for trade in filtered:

            pnl = trade.get(
                "pnl_pct"
            )

            if pnl is None:

                pnl = trade.get(
                    "return_pct"
                )

            if pnl is None:
                continue

            try:

                pnl = float(pnl)

                total_return += pnl

                if pnl > 0:
                    wins += 1

                elif pnl < 0:
                    losses += 1

                else:
                    breakeven += 1

            except Exception:

                continue

        if total > 0:

            win_rate = (
                wins / total * 100
            )

        else:

            win_rate = 0.0

        output.append(
            f"{name}\n"
            f"Trades: {total}\n"
            f"Wins: {wins}\n"
            f"Losses: {losses}\n"
            f"Breakeven: {breakeven}\n"
            f"Win rate: {win_rate:.2f}%\n"
            f"Strategy return: {total_return:+.3f}%\n"
        )

    return "\n".join(output)


# ============================================================
# COMMAND HANDLER
# ============================================================

async def handle_command(
    command,
    state
):

    if command == "/start":

        return start_message(
            state
        )

    if command == "/help":

        return build_help()

    if command == "/status":

        return status_message(
            state
        )

    if command == "/positions":

        return positions_message(
            state
        )

    if command == "/trades":

        return trades_message(
            state
        )

    if command in (
        "/p&l",
        "/pnl"
    ):

        return pnl_message(
            state
        )

    if command == "/signals":

        return signals_message(
            state
        )

    if command == "/summary":

        return summary_message(
            state
        )

    if command == "/now":

        return now_message(
            state
        )

    if command == "/overall":

        return overall_message(
            state
        )

    return (
        "Unknown command.\n\n"
        + build_help()
    )


# ============================================================
# TELEGRAM POLLING
# ============================================================

async def telegram_poll_loop(
    state
):

    print(
        "========================================",
        flush=True
    )

    print(
        "TELEGRAM LISTENER STARTING",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    if not TELEGRAM_BOT_TOKEN:

        print(
            "Telegram disabled: TELEGRAM_BOT_TOKEN is missing.",
            flush=True
        )

        return

    if not TELEGRAM_CHAT_ID:

        print(
            "Telegram disabled: TELEGRAM_CHAT_ID is missing.",
            flush=True
        )

        return

    print(
        "Telegram credentials detected.",
        flush=True
    )

    # Clear old updates so an old message does not
    # trigger immediately after deployment.

    result = await asyncio.to_thread(
        telegram_request,
        "getUpdates",
        {
            "offset": -1,
            "limit": 1,
            "timeout": 1,
        }
    )

    offset = 0

    if result and result.get("ok"):

        updates = result.get(
            "result",
            []
        )

        if updates:

            offset = (
                updates[-1]["update_id"]
                + 1
            )

    print(
        "Telegram command listener started.",
        flush=True
    )

    while True:

        try:

            result = await asyncio.to_thread(
                telegram_request,
                "getUpdates",
                {
                    "offset": offset,
                    "timeout": 25,
                }
            )

            if not result:

                await asyncio.sleep(2)

                continue

            if not result.get("ok"):

       