import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOptionContractsRequest
from alpaca.trading.enums import AssetStatus, ContractType

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


NY_TZ = ZoneInfo("America/New_York")


def main():

    print("========================================")
    print("OPTIONS TRADING BOT")
    print("STEP 8 - OPTION CONTRACT TEST")
    print("========================================")

    # ----------------------------------------
    # Credentials
    # ----------------------------------------

    api_key = os.environ.get("APCA_API_KEY_ID")
    api_secret = os.environ.get("APCA_API_SECRET_KEY")

    if not api_key:
        raise RuntimeError("Missing APCA_API_KEY_ID")

    if not api_secret:
        raise RuntimeError("Missing APCA_API_SECRET_KEY")

    # SAFETY LOCK
    if os.environ.get("ALPACA_PAPER", "true").lower() != "true":
        raise RuntimeError(
            "SAFETY ERROR: ALPACA_PAPER must be true."
        )

    print("Paper trading: ENABLED")

    # ----------------------------------------
    # Trading client
    # ----------------------------------------

    trading_client = TradingClient(
        api_key,
        api_secret,
        paper=True
    )

    account = trading_client.get_account()

    print("\nACCOUNT")
    print("----------------------------------------")
    print(f"Status:       {account.status}")
    print(f"Equity:       ${float(account.equity):,.2f}")
    print(f"Cash:         ${float(account.cash):,.2f}")
    print(f"Buying power: ${float(account.buying_power):,.2f}")

    # ----------------------------------------
    # Get current TSLA price
    # ----------------------------------------

    data_client = StockHistoricalDataClient(
        api_key,
        api_secret
    )

    latest_request = StockLatestTradeRequest(
        symbol_or_symbols=["TSLA"]
    )

    latest_trades = data_client.get_stock_latest_trade(
        latest_request
    )

    tsla_trade = latest_trades["TSLA"]
    tsla_price = float(tsla_trade.price)

    print("\nTSLA")
    print("----------------------------------------")
    print(f"Current price: ${tsla_price:.2f}")
    print(f"Trade time:    {tsla_trade.timestamp}")

    # ----------------------------------------
    # Current New York date
    # ----------------------------------------

    now_ny = datetime.now(NY_TZ)
    today = now_ny.date()

    print(f"NY date:       {today}")

    # ----------------------------------------
    # Get TSLA CALL contracts
    # ----------------------------------------

    print("\nFETCHING TSLA CALLS")
    print("----------------------------------------")

    call_request = GetOptionContractsRequest(
        underlying_symbols=["TSLA"],
        status=AssetStatus.ACTIVE,
        type=ContractType.CALL,
        expiration_date_gte=today,
        limit=10000
    )

    call_response = trading_client.get_option_contracts(
        call_request
    )

    calls = [
        contract
        for contract in call_response.option_contracts
        if contract.tradable
    ]

    # ----------------------------------------
    # Get TSLA PUT contracts
    # ----------------------------------------

    print("FETCHING TSLA PUTS")
    print("----------------------------------------")

    put_request = GetOptionContractsRequest(
        underlying_symbols=["TSLA"],
        status=AssetStatus.ACTIVE,
        type=ContractType.PUT,
        expiration_date_gte=today,
        limit=10000
    )

    put_response = trading_client.get_option_contracts(
        put_request
    )

    puts = [
        contract
        for contract in put_response.option_contracts
        if contract.tradable
    ]

    # ----------------------------------------
    # Sort contracts
    # ----------------------------------------

    calls.sort(
        key=lambda x: (
            x.expiration_date,
            abs(float(x.strike_price) - tsla_price)
        )
    )

    puts.sort(
        key=lambda x: (
            x.expiration_date,
            abs(float(x.strike_price) - tsla_price)
        )
    )

    # ----------------------------------------
    # Display nearest CALLS
    # ----------------------------------------

    print("\nNEAREST TSLA CALLS")
    print("----------------------------------------")

    for contract in calls[:10]:
        print(
            f"{contract.symbol} | "
            f"Strike=${float(contract.strike_price):.2f} | "
            f"Expiry={contract.expiration_date} | "
            f"Tradable={contract.tradable}"
        )

    # ----------------------------------------
    # Display nearest PUTS
    # ----------------------------------------

    print("\nNEAREST TSLA PUTS")
    print("----------------------------------------")

    for contract in puts[:10]:
        print(
            f"{contract.symbol} | "
            f"Strike=${float(contract.strike_price):.2f} | "
            f"Expiry={contract.expiration_date} | "
            f"Tradable={contract.tradable}"
        )

    # ----------------------------------------
    # Summary
    # ----------------------------------------

    print("\n========================================")
    print("STEP 8 COMPLETE")
    print("========================================")
    print(f"TSLA price:        ${tsla_price:.2f}")
    print(f"Tradable calls:    {len(calls)}")
    print(f"Tradable puts:     {len(puts)}")

    if calls:
        print(
            f"Nearest call:     {calls[0].symbol}"
        )

    if puts:
        print(
            f"Nearest put:      {puts[0].symbol}"
        )

    print("----------------------------------------")
    print("ORDER SUBMISSION: DISABLED")
    print("NO ORDERS WERE PLACED")
    print("========================================")

    # Keep Railway service alive.
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
