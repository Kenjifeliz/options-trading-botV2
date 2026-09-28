import os
import time

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


TICKERS = [
    "AAPL", "AMD", "PLTR", "TSLA", "NVDA",
    "QQQ", "GOOG", "SPY", "META", "ORCL",
    "HIMS", "JPM", "NKE", "ASTS"
]


def main():

    print("========================================")
    print("OPTIONS TRADING BOT")
    print("STEP 7 - BROKER + MARKET DATA TEST")
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
    # Trading API
    # ----------------------------------------

    trading_client = TradingClient(
        api_key,
        api_secret,
        paper=True
    )

    account = trading_client.get_account()

    print("\nACCOUNT")
    print("----------------------------------------")
    print(f"Status:          {account.status}")
    print(f"Equity:          ${float(account.equity):,.2f}")
    print(f"Cash:            ${float(account.cash):,.2f}")
    print(f"Buying power:    ${float(account.buying_power):,.2f}")
    print(f"Trading blocked: {account.trading_blocked}")

    # ----------------------------------------
    # Account configuration
    # ----------------------------------------

    config = trading_client.get_account_configurations()

    print("\nACCOUNT CONFIGURATION")
    print("----------------------------------------")
    print(
        "Max options trading level:",
        getattr(config, "max_options_trading_level", "unknown")
    )

    # ----------------------------------------
    # Check TSLA asset
    # ----------------------------------------

    print("\nCHECKING STOCK ASSETS")
    print("----------------------------------------")

    request = GetAssetsRequest(
        status=AssetStatus.ACTIVE,
        asset_class=AssetClass.US_EQUITY
    )

    assets = trading_client.get_all_assets(request)

    asset_map = {asset.symbol: asset for asset in assets}

    for ticker in TICKERS:
        asset = asset_map.get(ticker)

        if asset:
            print(
                f"{ticker:5} | "
                f"tradable={asset.tradable} | "
                f"options={getattr(asset, 'options_enabled', 'unknown')}"
            )
        else:
            print(f"{ticker:5} | NOT FOUND")

    # ----------------------------------------
    # Live stock market data test
    # ----------------------------------------

    print("\nLIVE MARKET DATA")
    print("----------------------------------------")

    data_client = StockHistoricalDataClient(
        api_key,
        api_secret
    )

    latest_request = StockLatestTradeRequest(
        symbol_or_symbols=["TSLA", "SPY", "QQQ"]
    )

    latest_trades = data_client.get_stock_latest_trade(
        latest_request
    )

    for symbol, trade in latest_trades.items():
        print(
            f"{symbol}: "
            f"${trade.price:.2f} "
            f"at {trade.timestamp}"
        )

    # ----------------------------------------
    # Safety confirmation
    # ----------------------------------------

    print("\n========================================")
    print("STEP 7 COMPLETE")
    print("========================================")
    print("Broker connection:      OK")
    print("Paper mode:             ON")
    print("Order submission:       DISABLED")
    print("Live stock data test:   COMPLETE")
    print("========================================")

    # Keep Railway service alive.
    while True:
        time.sleep(300)


if __name__ == "__main__":
    main()
