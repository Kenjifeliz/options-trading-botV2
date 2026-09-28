import os
import time
from alpaca.trading.client import TradingClient


def main():
    print("========================================")
    print("OPTIONS TRADING BOT")
    print("========================================")
    print("Starting Alpaca connection...")

    api_key = os.environ.get("APCA_API_KEY_ID")
    api_secret = os.environ.get("APCA_API_SECRET_KEY")
    paper = os.environ.get("ALPACA_PAPER", "true").lower() == "true"

    if not api_key:
        raise RuntimeError("Missing APCA_API_KEY_ID")

    if not api_secret:
        raise RuntimeError("Missing APCA_API_SECRET_KEY")

    if not paper:
        raise RuntimeError(
            "SAFETY CHECK FAILED: ALPACA_PAPER is not true."
        )

    print("Paper trading mode: ENABLED")
    print("Connecting to Alpaca...")

    trading_client = TradingClient(
        api_key,
        api_secret,
        paper=True
    )

    account = trading_client.get_account()

    print("")
    print("========================================")
    print("ALPACA CONNECTION SUCCESSFUL")
    print("========================================")
    print(f"Account status: {account.status}")
    print(f"Account ID:     {account.id}")
    print(f"Equity:         ${float(account.equity):,.2f}")
    print(f"Cash:           ${float(account.cash):,.2f}")
    print(f"Buying power:   ${float(account.buying_power):,.2f}")
    print(f"Trading blocked: {account.trading_blocked}")
    print("========================================")
    print("")
    print("NO ORDERS WILL BE PLACED.")
    print("Connection test is complete.")

    # Keep the service alive for now.
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
