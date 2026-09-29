import os
import asyncio
import urllib.request
import urllib.parse


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials are missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }).encode("utf-8")

    try:
        request = urllib.request.Request(url, data=data, method="POST")

        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    except Exception as e:
        print(f"Telegram error: {e}")


HELP_MESSAGE = """🤖 BOT COMMANDS

/start — Bot status, mode & portfolio size

/status — Full bot connection & operating status

/positions — Current open positions & unrealized P&L

/trades — Today's trades & individual results

/p&l — Today's realized, unrealized & total P&L

/signals — Recent strategy signals & their status

/summary — Full daily overview

/now — What the bot is currently watching and what conditions are still needed for entry

/help — Show this command list"""


async def send_help():
    await asyncio.to_thread(send_telegram_message, HELP_MESSAGE)
