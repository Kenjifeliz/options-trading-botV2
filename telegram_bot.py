import os
import json
import asyncio
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ============================================================
# TELEGRAM SETTINGS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

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

ALPACA_PAPER = (
    os.environ.get("ALPACA_PAPER", "true").lower() == "true"
)

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")

_trading_client = None


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
            f"Telegram API error: {error}",
            flush=True,
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
            "text": str(text),
        },
    )


# ============================================================
# BASIC HELPERS
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


def format_money(value):

    try:

        value = float(value)

        if value >= 0:
            return f"+${value:,.2f}"

        return f"-${abs(value):,.2f}"

    except Exception:
        return "N/A"


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


def get_active_entries(state):

    return state.get(
        "active_entries",
        {},
    )


def get_all_trades(state):

    return state.get(
        "all_trades",
        [],
    )


def get_history(state):

    return state.get(
        "five_minute_history",
        {},
    )


def count_active_positions(state):

    active = get_active_entries(state)

    total = 0

    for entries in active.values():

        try:
            total += len(entries)

        except Exception:
            pass

    return total


def get_today_trades(state):

    today = datetime.now(
        TIMEZONE
    ).date()

    result = []

    for trade in get_all_trades(state):

        entry_time = parse_datetime(
            trade.get("entry_time")
        )

        if entry_time is None:
            continue

        if entry_time.date() == today:
            result.append(trade)

    return result


def get_trade_pnl_dollars(trade):

    value = trade.get("pnl")

    if value is None:
        return None

    try:
        return float(value)

    except Exception:
        return None


def get_trade_status(trade):

    status = trade.get("status")

    if status:
        return str(status).upper()

    if trade.get("exit_time") is not None:
        return "CLOSED"

    return "OPEN"


# ============================================================
# /HELP
# ============================================================

def build_help():

    return (
        "🤖 BOT COMMANDS\n\n"

        "/start — Bot status, mode & portfolio size\n"
        "/status — Full bot connection & operating status\n"
        "/positions — Current open positions\n"
        "/trades — Today's trades\n"
        "/p&l — Today's option P&L\n"
        "/signals — Recent recorded entries/signals\n"
        "/summary — Full daily overview\n"
        "/now — Current 5-minute market conditions\n"
        "/overall — Weekly, monthly & yearly performance\n"
        "/help — Show this command list"
    )


# ============================================================
# /START
# ============================================================

def start_message(state):

    mode = (
        "PAPER"
        if ALPACA_PAPER
        else "LIVE"
    )

    equity = "N/A"

    client = get_trading_client()

    if client is not None:

        try:

            account = client.get_account()

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

    mode = (
        "PAPER"
        if ALPACA_PAPER
        else "LIVE"
    )

    active_count = count_active_positions(
        state
    )

    today_trades = get_today_trades(
        state
    )

    alpaca_status = "DISCONNECTED"

    client = get_trading_client()

    if client is not None:

        try:

            client.get_account()

            alpaca_status = "CONNECTED"

        except Exception:

            alpaca_status = "DISCONNECTED"

    history = get_history(state)

    latest_time = None

    for ticker in TICKERS:

        candles = history.get(
            ticker,
            [],
        )

        if not candles:
            continue

        candle = candles[-1]

        timestamp = (
            candle.get("timestamp")
            or candle.get("time")
            or candle.get("datetime")
        )

        dt = parse_datetime(timestamp)

        if dt is not None:

            if (
                latest_time is None
                or dt > latest_time
            ):
                latest_time = dt

    if latest_time is not None:

        last_update = latest_time.strftime(
            "%H:%M:%S ET"
        )

    else:

        last_update = "N/A"

    return (
        "🟢 BOT STATUS: ONLINE\n\n"
        f"Mode: {mode}\n"
        "Market data: CONNECTED\n"
        f"Alpaca: {alpaca_status}\n"
        "Telegram: CONNECTED\n"
        f"Last market update: {last_update}\n"
        f"Active positions: {active_count}\n"
        f"Trades today: {len(today_trades)}"
    )


# ============================================================
# /POSITIONS
# ============================================================

def positions_message(state):

    active = get_active_entries(
        state
    )

    lines = [
        "📊 OPEN POSITIONS",
        "",
    ]

    total = 0

    for ticker in TICKERS:

        entries = active.get(
            ticker,
            [],
        )

        for entry in entries:

            total += 1

            option_symbol = entry.get(
                "option_symbol",
                "N/A",
            )

            contracts = entry.get(
                "contracts",
                0,
            )

            entry_option = entry.get(
                "entry_option"
            )

            entry_underlying = entry.get(
                "entry_underlying"
            )

            entry_time = parse_datetime(
                entry.get("entry_time")
            )

            lines.append(
                f"🟢 {ticker} — CALL"
            )

            lines.append(
                f"Option: {option_symbol}"
            )

            lines.append(
                f"Contracts: {contracts}"
            )

            if entry_underlying is not None:

                lines.append(
                    "Underlying entry: $"
                    + format_number(
                        entry_underlying,
                        2,
                    )
                )

            if entry_option is not None:

                lines.append(
                    "Option entry: $"
                    + format_number(
                        entry_option,
                        4,
                    )
                )

            if entry_time is not None:

                lines.append(
                    "Entry: "
                    + entry_time.strftime(
                        "%H:%M:%S ET"
                    )
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

def trades_message(state):

    trades = get_today_trades(
        state
    )

    lines = [
        "📈 TODAY'S TRADES",
        "",
        f"{len(trades)} trades",
        "",
    ]

    wins = 0
    losses = 0
    open_trades = 0

    for index, trade in enumerate(
        trades,
        start=1,
    ):

        ticker = trade.get(
            "ticker",
            "?",
        )

        option_symbol = trade.get(
            "option_symbol",
            "",
        )

        pnl = get_trade_pnl_dollars(
            trade
        )

        status = get_trade_status(
            trade
        )

        if status == "OPEN":

            open_trades += 1

        elif pnl is not None:

            if pnl > 0:
                wins += 1

            elif pnl < 0:
                losses += 1

        lines.append(
            f"{index}. {ticker} — CALL"
        )

        if option_symbol:
            lines.append(
                f"   {option_symbol}"
            )

        if status == "OPEN":

            lines.append(
                "   Status: OPEN"
            )

        elif pnl is not None:

            lines.append(
                "   P&L: "
                + format_money(pnl)
            )

        else:

            lines.append(
                f"   Status: {status}"
            )

        exit_time = parse_datetime(
            trade.get("exit_time")
        )

        if exit_time is not None:

            lines.append(
                "   Exit: "
                + exit_time.strftime(
                    "%H:%M:%S ET"
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

def pnl_message(state):

    trades = get_today_trades(
        state
    )

    realized = 0.0
    wins = 0
    losses = 0
    closed = 0

    for trade in trades:

        if get_trade_status(trade) == "OPEN":
            continue

        pnl = get_trade_pnl_dollars(
            trade
        )

        if pnl is None:
            continue

        realized += pnl

        closed += 1

        if pnl > 0:
            wins += 1

        elif pnl < 0:
            losses += 1

    open_count = count_active_positions(
        state
    )

    portfolio = "N/A"

    client = get_trading_client()

    if client is not None:

        try:

            account = client.get_account()

            portfolio = format_number(
                account.equity
            )

        except Exception:
            pass

    return (
        "💰 TODAY'S P&L\n\n"
        f"Realized option P&L: "
        f"{format_money(realized)}\n"
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

    trades = get_today_trades(
        state
    )

    if not trades:

        return (
            "📡 RECENT SIGNALS\n\n"
            "No entered signals recorded today.\n\n"
            "Note: the current main.py does not "
            "persist rejected signals separately."
        )

    recent = trades[-10:]

    lines = [
        "📡 RECENT SIGNALS / ENTRIES",
        "",
    ]

    for trade in recent:

        ticker = trade.get(
            "ticker",
            "?",
        )

        signal_time = parse_datetime(
            trade.get("signal_time")
        )

        entry_time = parse_datetime(
            trade.get("entry_time")
        )

        if signal_time is not None:

            signal_text = signal_time.strftime(
                "%H:%M:%S ET"
            )

        else:

            signal_text = "N/A"

        if entry_time is not None:

            entry_text = entry_time.strftime(
                "%H:%M:%S ET"
            )

        else:

            entry_text = "N/A"

        lines.append(
            f"{ticker} — LONG CALL"
        )

        lines.append(
            f"Signal: {signal_text}"
        )

        lines.append(
            f"Entry: {entry_text}"
        )

        lines.append("Status: ENTERED")

        lines.append("")

    lines.append(
        f"Total shown: {len(recent)}"
    )

    return "\n".join(lines)


# ============================================================
# /SUMMARY
# ============================================================

def summary_message(state):

    today_trades = get_today_trades(
        state
    )

    active_count = count_active_positions(
        state
    )

    closed_pnl = 0.0
    wins = 0
    losses = 0
    closed = 0

    for trade in today_trades:

        if get_trade_status(trade) == "OPEN":
            continue

        pnl = get_trade_pnl_dollars(
            trade
        )

        if pnl is None:
            continue

        closed += 1
        closed_pnl += pnl

        if pnl > 0:
            wins += 1

        elif pnl < 0:
            losses += 1

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
        f"• Closed: {closed}\n"
        f"• Wins: {wins}\n"
        f"• Losses: {losses}\n"
        f"• Open positions: {active_count}\n"
        f"• Realized option P&L: "
        f"{format_money(closed_pnl)}\n\n"

        "Strategy\n"
        "• Direction: LONG ONLY\n"
        "• Option: CALL ONLY\n"
        "• Timeframe: 5 minutes\n"
        "• Start: 10:00 ET\n"
        "• VWAP filter: 0.28%\n"
        "• Exit: CLOSE below EMA9\n"
        "• EOD: Final session candle"
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
        "",
    ]

    found = 0

    for ticker in TICKERS:

        candles = history.get(
            ticker,
            [],
        )

        if not candles:
            continue

        candle = candles[-1]

        close = candle.get(
            "close"
        )

        if close is None:
            continue

        vwap = candle.get(
            "vwap"
        )

        ema9 = candle.get(
            "ema9"
        )

        text = (
            f"{ticker}: "
            f"${format_number(close, 2)}"
        )

        if vwap is not None:

            text += (
                f" | VWAP "
                f"${format_number(vwap, 2)}"
            )

        if ema9 is not None:

            text += (
                f" | EMA9 "
                f"${format_number(ema9, 2)}"
            )

        lines.append(text)

        found += 1

    if found == 0:

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

    period_starts = {
        "TODAY": now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),

        "THIS WEEK": (
            now
            - timedelta(
                days=now.weekday()
            )
        ).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),

        "THIS MONTH": now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),

        "THIS YEAR": now.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),
    }

    output = [
        "📊 OVERALL PERFORMANCE",
        "",
    ]

    for name, start_date in period_starts.items():

        filtered = []

        for trade in trades:

            entry_time = parse_datetime(
                trade.get("entry_time")
            )

            if entry_time is None:
                continue

            if entry_time < start_date:
                continue

            if get_trade_status(trade) == "OPEN":
                continue

            filtered.append(
                trade
            )

        total = len(filtered)
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
            f"Option P&L: "
            f"{format_money(total_pnl)}\n"
        )

    return "\n".join(output)


# ============================================================
# COMMAND HANDLER
# ============================================================

async def handle_command(
    command,
    state,
):

    command = (
        command
        .strip()
        .split()[0]
        .lower()
    )

    if command == "/start":
        return start_message(state)

    if command == "/help":
        return build_help()

    if command == "/status":
        return status_message(state)

    if command == "/positions":
        return positions_message(state)

    if command == "/trades":
        return trades_message(state)

    if command in (
        "/p&l",
        "/pnl",
    ):
        return pnl_message(state)

    if command == "/signals":
        return signals_message(state)

    if command == "/summary":
        return summary_message(state)

    if command == "/now":
        return now_message(state)

    if command == "/overall":
        return overall_message(state)

    return (
        "Unknown command.\n\n"
        + build_help()
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

    me = await asyncio.to_thread(
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

    result = await asyncio.to_thread(
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

            result = await asyncio.to_thread(
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

                text = message.get(
                    "text",
                    "",
                ).strip()

                if not text:
                    continue

                if not text.startswith("/"):
                    continue

                command = (
                    text
                    .split()[0]
                    .split("@")[0]
                    .lower()
                )

                print(
                    f"Telegram command received: {command}",
                    flush=True,
                )

                try:

                    response = await handle_command(
                        command,
                        state,
                    )

                    if response:

                        send_message(
                            response
                        )

                except Exception as error:

                    print(
                        "Telegram command error: "
                        + str(error),
                        flush=True,
                    )

                    send_message(
                        "⚠️ Telegram command error:\n"
                        + str(error)
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
