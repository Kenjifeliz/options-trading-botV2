import asyncio
import main
from telegram_bot import telegram_poll_loop


async def launcher():

    print(
        "========================================",
        flush=True
    )

    print(
        "BOT LAUNCHER STARTING",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    # One shared state object.
    #
    # Telegram reads the same strategy state
    # used by the trading engine.

    state = {
        "active_entries": main.active_entries,
        "all_trades": getattr(
            main,
            "all_trades",
            []
        ),
        "five_minute_history": main.five_minute_history,
        "signals": getattr(
            main,
            "signals",
            []
        ),
        "trading_client": getattr(
            main,
            "trading_client",
            None
        ),
    }

    trading_task = asyncio.create_task(
        main.main()
    )

    telegram_task = asyncio.create_task(
        telegram_poll_loop(
            state
        )
    )

    print(
        "Trading engine task started.",
        flush=True
    )

    print(
        "Telegram task started.",
        flush=True
    )

    await asyncio.gather(
        trading_task,
        telegram_task
    )


if __name__ == "__main__":

    asyncio.run(
        launcher()
    )