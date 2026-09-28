import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOptionContractsRequest
from alpaca.trading.enums import AssetStatus, ContractType

from alpaca.data.historical import (
    StockHistoricalDataClient,
    OptionHistoricalDataClient,
)
from alpaca.data.requests import (
    StockLatestTradeRequest,
    OptionLatestQuoteRequest,
)


NY_TZ = ZoneInfo("America/New_York")


def main():

    print("========================================")
    print("OPTIONS TRADING BOT")
    print("STEP 9 - LIVE OPTION QUOTE TEST")
    print("========================================")

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
    # Clients
    # ----------------------------------------

    trading_client = TradingClient(
        api_key,
        api_secret,
        paper=True
    )

    stock_client = StockHistoricalDataClient(
        api_key,
        api_secret
    )

    option_client = OptionHistoricalDataClient(
        api_key,
        api_secret
    )

    # ----------------------------------------
    # Account
    # ----------------------------------------

    account = trading_client.get_account()

    print("\nACCOUNT")
    print("----------------------------------------")
    print(f"Status:       {account.status}")
    print(f"Equity:       ${float(account.equity):,.2f}")
    print(f"Cash:         ${float(account.cash):,.2f}")
    print(f"Buying power: ${float(account.buying_power):,.2f}")

    # ----------------------------------------
    # Get live TSLA price
    # ----------------------------------------

    stock_request = StockLatestTradeRequest(
        symbol_or_symbols=["TSLA"]
    )

    stock_trades = stock_client.get_stock_latest_trade(
        stock_request
    )

    tsla_price = float(stock_trades["TSLA"].price)

    print("\nTSLA")
    print("----------------------------------------")
    print(f"Underlying price: ${tsla_price:.2f}")

    # ----------------------------------------
    # Get today's nearest contracts
    # ----------------------------------------

    today = datetime.now(NY_TZ).date()

    call_request = GetOptionContractsRequest(
        underlying_symbols=["TSLA"],
        status=AssetStatus.ACTIVE,
        type=ContractType.CALL,
        expiration_date_gte=today,
        limit=10000
    )

    put_request = GetOptionContractsRequest(
        underlying_symbols=["TSLA"],
        status=AssetStatus.ACTIVE,
        type=ContractType.PUT,
        expiration_date_gte=today,
        limit=10000
    )

    calls = trading_client.get_option_contracts(
        call_request
    ).option_contracts

    puts = trading_client.get_option_contracts(
        put_request
    ).option_contracts

    calls = [
        c for c in calls
        if c.tradable
    ]

    puts = [
        p for p in puts
        if p.tradable
    ]

    # ----------------------------------------
    # Select nearest strike
    # ----------------------------------------

    nearest_call = min(
        calls,
        key=lambda c: (
            c.expiration_date,
            abs(float(c.strike_price) - tsla_price)
        )
    )

    nearest_put = min(
        puts,
        key=lambda p: (
            p.expiration_date,
            abs(float(p.strike_price) - tsla_price)
        )
    )

    call_symbol = nearest_call.symbol
    put_symbol = nearest_put.symbol

    print("\nSELECTED CONTRACTS")
    print("----------------------------------------")
    print(
        f"CALL: {call_symbol} | "
        f"Strike=${float(nearest_call.strike_price):.2f} | "
        f"Expiry={nearest_call.expiration_date}"
    )

    print(
        f"PUT:  {put_symbol} | "
        f"Strike=${float(nearest_put.strike_price):.2f} | "
        f"Expiry={nearest_put.expiration_date}"
    )

    # ----------------------------------------
    # Get live option quotes
    # ----------------------------------------

    print("\nLIVE OPTION QUOTES")
    print("----------------------------------------")

    quote_request = OptionLatestQuoteRequest(
        symbol_or_symbols=[
            call_symbol,
            put_symbol
        ]
    )

    quotes = option_client.get_option_latest_quote(
        quote_request
    )

    # ----------------------------------------
    # Display CALL quote
    # ----------------------------------------

    call_quote = quotes[call_symbol]

    print("\nCALL")
    print("----------------------------------------")
    print(f"Symbol:       {call_symbol}")
    print(f"Bid:          ${float(call_quote.bid_price):.2f}")
    print(f"Ask:          ${float(call_quote.ask_price):.2f}")
    print(f"Bid size:     {call_quote.bid_size}")
    print(f"Ask size:     {call_quote.ask_size}")
    print(f"Timestamp:    {call_quote.timestamp}")

    # ----------------------------------------
    # Display PUT quote
    # ----------------------------------------

    put_quote = quotes[put_symbol]

    print("\nPUT")
    print("----------------------------------------")
    print(f"Symbol:       {put_symbol}")
    print(f"Bid:          ${float(put_quote.bid_price):.2f}")
    print(f"Ask:          ${float(put_quote.ask_price):.2f}")
    print(f"Bid size:     {put_quote.bid_size}")
    print(f"Ask size:     {put_quote.ask_size}")
    print(f"Timestamp:    {put_quote.timestamp}")

    # ----------------------------------------
    # Calculate 25-contract notional
    # ----------------------------------------

    call_ask = float(call_quote.ask_price)
    put_ask = float(put_quote.ask_price)

    call_cost = call_ask * 100 * 25
    put_cost = put_ask * 100 * 25

    print("\n25-CONTRACT COST")
    print("----------------------------------------")
    print(f"CALL ask cost: ${call_cost:,.2f}")
    print(f"PUT ask cost:  ${put_cost:,.2f}")

    # ----------------------------------------
    # Safety confirmation
    # ----------------------------------------

    print("\n========================================")
    print("STEP 9 COMPLETE")
    print("========================================")
    print("Underlying data:       OK")
    print("Option contracts:      OK")
    print("Live option quotes:    OK")
    print("Order submission:      DISABLED")
    print("NO ORDERS WERE PLACED")
    print("========================================")

    # Keep Railway service alive.
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
