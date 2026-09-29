import os
import asyncio
import json
import urllib.request
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("America/New_York")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _post_telegram(method, params):
    if not TELEGRAM_BOT_TOKEN:
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    data = urllib.parse.urlencode(params).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


async def send_message(message, chat_id=None):
    target = str(chat_id or TELEGRAM_CHAT_ID or "")

    if not TELEGRAM_BOT_TOKEN or not target:
        return

    try:
        await asyncio.to_thread(
            _post_telegram,
            "sendMessage",
            {
                "chat_id": target,
                "text": message,
            },
        )
    except Exception as exc:
        print(f"Telegram send error: {exc}")


def _get_updates(offset):
    return _post_telegram(
        "getUpdates",
        {
            "offset": offset,
            "timeout": 20,
            "allowed_updates": json.dumps(["message"]),
        },
    )


def _money(value):
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def _signed_pct(value):
    return f"{value:+.2f}%"


def build_help():
    return """🤖 BOT COMMANDS

/start — Bot status, mode & portfolio size

/status — Full bot connection & operating status

/positions — Current open positions & unrealized P&L

/trades — Today's trades & individual results

/p&l — Today's realized, unrealized & total P&L

/signals — Recent strategy signals & their status

/summary — Full daily overview

/overall — Weekly, monthly & yearly performance

/now — What the bot is currently watching and what conditions are still needed for entry

/help — Show this command list"""


def _latest_price(state, ticker):
    rows = state["history"].get(ticker, [])
    if not rows:
        return None
    return float(rows[-1]["close"])


def _position_lines(state):
    lines = ["📊 OPEN POSITIONS", ""]
    positions = []

    for ticker in state["tickers"]:
        for entry in state["active_entries"].get(ticker, []):
            current = _latest_price(state, ticker)
            entry_price = float(entry["entry_price"])
            direction = entry["direction"]

            if current is None:
                pct = None
            elif direction == "LONG":
                pct = (current / entry_price - 1) * 100
            else:
                pct = (entry_price / current - 1) * 100

            held = max(
                0,
                int(
                    (
                        state["last_market_update"] - entry["entry_time"]
                    ).total_seconds() // 60
                ),
            ) if state["last_market_update"] else 0

            positions.append((ticker, direction, entry_price, current, pct, held))

    if not positions:
        lines.append("No open strategy positions.")
        return "\n".join(lines)

    total_pct = 0.0

    for ticker, direction, entry_price, current, pct, held in positions:
        lines.append(
            f"{ticker} — {direction}\n"
            f"Entry: {_money(entry_price)}\n"
            f"Current: {_money(current)}\n"
            f"P&L: {_signed_pct(pct) if pct is not None else 'N/A'}\n"
            f"Hold: {held} min\n"
        )
        if pct is not None:
            total_pct += pct

    lines.append(f"Total: {len(positions)} positions")
    lines.append(f"Strategy unrealized P&L: {_signed_pct(total_pct)}")
    lines.append("")
    lines.append("Note: option orders are currently disabled, so this is underlying-strategy P&L.")

    return "\n".join(lines)


def _trades_message(state):
    trades = state["trades_today"]

    lines = ["📈 TODAY'S TRADES", ""]
    lines.append(f"{len(trades)} trades")
    lines.append("")

    if not trades:
        lines.append("No completed or entered trades today.")
        return "\n".join(lines)

    for i, trade in enumerate(trades[-20:], 1):
        direction = trade["direction"]
        status = trade["status"]
        pct = trade.get("return_pct")

        result = (
            _signed_pct(pct)
            if pct is not None
            else "OPEN"
        )

        lines.append(
            f"{i}. {trade['ticker']} — {direction} — {result} — {status}"
        )

    completed = [t for t in trades if t["status"] == "CLOSED"]
    wins = sum(1 for t in completed if (t.get("return_pct") or 0) > 0)
    losses = sum(1 for t in completed if (t.get("return_pct") or 0) < 0)
    open_count = sum(1 for t in trades if t["status"] == "OPEN")

    realized = sum(t.get("return_pct", 0.0) or 0.0 for t in completed)

    lines.extend(
        [
            "",
            f"Realized strategy return: {_signed_pct(realized)}",
            f"Wins: {wins}",
            f"Losses: {losses}",
            f"Open: {open_count}",
            "",
            "Note: option orders are currently disabled, so dollar option P&L is not available yet.",
        ]
    )

    return "\n".join(lines)


def _pnl_message(state):
    completed = [t for t in state["trades_today"] if t["status"] == "CLOSED"]
    realized = sum(t.get("return_pct", 0.0) or 0.0 for t in completed)

    unrealized = 0.0
    open_count = 0

    for ticker in state["tickers"]:
        for entry in state["active_entries"].get(ticker, []):
            current = _latest_price(state, ticker)
            if current is None:
                continue

            entry_price = float(entry["entry_price"])
            if entry["direction"] == "LONG":
                unrealized += (current / entry_price - 1) * 100
            else:
                unrealized += (entry_price / current - 1) * 100
            open_count += 1

    total = realized + unrealized

    return (
        "💰 TODAY'S P&L\n\n"
        f"Realized strategy return: {_signed_pct(realized)}\n"
        f"Unrealized strategy return: {_signed_pct(unrealized)}\n"
        f"Total strategy return: {_signed_pct(total)}\n\n"
        f"Trades: {len(state['trades_today'])}\n"
        f"Wins: {sum(1 for t in completed if (t.get('return_pct') or 0) > 0)}\n"
        f"Losses: {sum(1 for t in completed if (t.get('return_pct') or 0) < 0)}\n"
        f"Open positions: {open_count}\n\n"
        f"Portfolio: {_money(state.get('portfolio_equity'))}\n\n"
        "Note: option orders are currently disabled, so dollar option P&L is not available yet."
    )


def _signals_message(state):
    signals = state["signals_today"][-10:]

    lines = ["📡 RECENT SIGNALS", ""]

    if not signals:
        lines.append("No qualifying signals today.")
        return "\n".join(lines)

    for signal in signals:
        lines.extend(
            [
                f"{signal['ticker']} — {signal['direction']}",
                f"Signal: {signal['time'].strftime('%H:%M')} ET",
                f"VWAP: {_money(signal['vwap'])}",
                f"Signal close: {_money(signal['close'])}",
                f"EMA9: {_money(signal['ema9'])}",
                f"VWAP distance: {signal['distance_pct']:.2f}%",
                f"Status: {signal['status']}",
                "",
            ]
        )

    entered = sum(1 for s in state["signals_today"] if s["status"] == "ENTERED")
    rejected = sum(1 for s in state["signals_today"] if s["status"].startswith("REJECTED"))
    pending = sum(1 for s in state["signals_today"] if s["status"] == "PENDING ENTRY")

    lines.extend(
        [
            f"Total signals: {len(state['signals_today'])}",
            f"Entered: {entered}",
            f"Rejected: {rejected}",
            f"Pending: {pending}",
        ]
    )

    return "\n".join(lines)


def _summary_message(state):
    completed = [t for t in state["trades_today"] if t["status"] == "CLOSED"]
    realized = sum(t.get("return_pct", 0.0) or 0.0 for t in completed)

    open_count = sum(
        len(state["active_entries"].get(ticker, []))
        for ticker in state["tickers"]
    )

    unrealized = 0.0
    for ticker in state["tickers"]:
        for entry in state["active_entries"].get(ticker, []):
            current = _latest_price(state, ticker)
            if current is None:
                continue
            entry_price = float(entry["entry_price"])
            if entry["direction"] == "LONG":
                unrealized += (current / entry_price - 1) * 100
            else:
                unrealized += (entry_price / current - 1) * 100

    total = realized + unrealized

    entered = sum(1 for s in state["signals_today"] if s["status"] == "ENTERED")
    rejected = sum(1 for s in state["signals_today"] if s["status"].startswith("REJECTED"))
    pending = sum(1 for s in state["signals_today"] if s["status"] == "PENDING ENTRY")

    return (
        "📊 TODAY'S SUMMARY\n\n"
        "Bot\n"
        f"- Status: {'🟢 ONLINE' if state['online'] else '🔴 OFFLINE'}\n"
        f"- Mode: {'PAPER' if state['paper_mode'] else 'LIVE'}\n"
        f"- Portfolio: {_money(state.get('portfolio_equity'))}\n\n"
        "Trading\n"
        f"- Trades: {len(state['trades_today'])}\n"
        f"- Wins: {sum(1 for t in completed if (t.get('return_pct') or 0) > 0)}\n"
        f"- Losses: {sum(1 for t in completed if (t.get('return_pct') or 0) < 0)}\n"
        f"- Open positions: {open_count}\n\n"
        "P&L\n"
        f"- Realized strategy: {_signed_pct(realized)}\n"
        f"- Unrealized strategy: {_signed_pct(unrealized)}\n"
        f"- Total strategy: {_signed_pct(total)}\n\n"
        "Signals\n"
        f"- Total: {len(state['signals_today'])}\n"
        f"- Entered: {entered}\n"
        f"- Rejected: {rejected}\n"
        f"- Pending: {pending}\n"
    )



def _overall_message(state):
    trades = state.get("all_trades", [])
    now = datetime.now(TIMEZONE)
    periods = [
        ("TODAY", now.date()),
        ("THIS WEEK", now.date() - __import__("datetime").timedelta(days=now.weekday())),
        ("THIS MONTH", now.replace(day=1).date()),
        ("THIS YEAR", now.replace(month=1, day=1).date()),
    ]

    lines = ["📊 OVERALL PERFORMANCE", ""]

    for label, start_date in periods:
        period_trades = [
            t for t in trades
            if t["entry_time"].date() >= start_date
            and t["status"] == "CLOSED"
        ]

        wins = sum(1 for t in period_trades if (t.get("return_pct") or 0) > 0)
        losses = sum(1 for t in period_trades if (t.get("return_pct") or 0) < 0)
        breakeven = sum(1 for t in period_trades if (t.get("return_pct") or 0) == 0)
        total = len(period_trades)
        win_rate = (wins / total * 100) if total else 0.0
        pnl = sum(t.get("return_pct", 0.0) or 0.0 for t in period_trades)

        lines.extend([
            label,
            f"Trades: {total}",
            f"Wins: {wins}",
            f"Losses: {losses}",
            f"Breakeven: {breakeven}",
            f"Win rate: {win_rate:.2f}%",
            f"P&L: {_signed_pct(pnl)}",
            "",
        ])

    lines.append(
        "Note: P&L shown here is strategy return, because option orders are currently disabled."
    )
    return "\n".join(lines)


def _now_message(state):
    lines = ["👀 NOW WATCHING", ""]

    for ticker in state["tickers"]:
        info = state["watching"].get(ticker)

        if not info:
            lines.append(f"{ticker} — ⏳ Waiting for market data")
            continue

        direction = info.get("direction", "—")
        status = info.get("status", "WAITING")
        missing = info.get("waiting_for", "Qualifying VWAP cross")

        price = info.get("price")
        vwap = info.get("vwap")
        ema9 = info.get("ema9")

        lines.append(
            f"{ticker} — {direction}\n"
            f"Price: {_money(price)} | VWAP: {_money(vwap)} | EMA9: {_money(ema9)}\n"
            f"Waiting: {missing}\n"
            f"Status: {status}\n"
        )

    return "\n".join(lines)


async def handle_command(command, state):
    if command == "/help":
        return build_help()

    if command == "/start":
        return (
            "🟢 BOT STATUS: ONLINE\n"
            f"MODE: {'PAPER' if state['paper_mode'] else 'LIVE'}\n"
            f"PORTFOLIO SIZE: {_money(state.get('portfolio_equity'))}"
        )

    if command == "/status":
        last_update = state.get("last_market_update")
        last_text = (
            last_update.strftime("%H:%M:%S ET")
            if last_update
            else "N/A"
        )

        active = sum(
            len(state["active_entries"].get(ticker, []))
            for ticker in state["tickers"]
        )

        return (
            f"{'🟢' if state['online'] else '🔴'} BOT STATUS: "
            f"{'ONLINE' if state['online'] else 'OFFLINE'}\n\n"
            f"Mode: {'PAPER' if state['paper_mode'] else 'LIVE'}\n"
            "Market data: CONNECTED\n"
            f"Alpaca: {'CONNECTED' if state['alpaca_connected'] else 'DISCONNECTED'}\n"
            "Telegram: CONNECTED\n"
            f"Last market update: {last_text}\n"
            f"Active positions: {active}\n"
            f"Trades today: {len(state['trades_today'])}"
        )

    if command == "/positions":
        return _position_lines(state)

    if command == "/trades":
        return _trades_message(state)

    if command in ("/p&l", "/pnl"):
        return _pnl_message(state)

    if command == "/signals":
        return _signals_message(state)

    if command == "/summary":
        return _summary_message(state)

    if command == "/overall":
        return _overall_message(state)

    if command == "/now":
        return _now_message(state)

    return "Unknown command. Send /help to see the available commands."


async def telegram_poll_loop(state):
    if not TELEGRAM_BOT_TOKEN:
        print("Telegram is disabled: TELEGRAM_BOT_TOKEN is missing.")
        return

    print("Telegram command listener started.")

    offset = 0

    while True:
        try:
            result = await asyncio.to_thread(_get_updates, offset)

            if not result or not result.get("ok"):
                await asyncio.sleep(3)
                continue

            for update in result.get("result", []):
                offset = update["update_id"] + 1

                message = update.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = str(chat.get("id", ""))

                if TELEGRAM_CHAT_ID and chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                text = (message.get("text") or "").strip().lower()

                if not text.startswith("/"):
                    continue

                command = text.split()[0].split("@")[0]

                response = await handle_command(command, state)
                await send_message(response, chat_id=chat_id)

        except Exception as exc:
            print(f"Telegram listener error: {exc}")
            await asyncio.sleep(5)
