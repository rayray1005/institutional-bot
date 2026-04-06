import asyncio
import json
import os

import websockets
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

WATCHLIST = [
    "SPY",
    "TSLA",
    "INTC",
    "META",
    "NVDA",
    "SNDK",
    "MU",
    "AMZN",
    "PLTR",
    "AAPL",
    "NFLX",
    "IWM",
    "AMD",
]

def get_underlying_from_option_symbol(option_symbol: str) -> str:
    """
    Polygon options symbols usually look like:
    O:SPY250117C00500000
    We grab the part after O: and before the date block starts.
    """
    if option_symbol.startswith("O:"):
        raw = option_symbol[2:]
    else:
        raw = option_symbol

    i = 0
    while i < len(raw) and not raw[i].isdigit():
        i += 1

    return raw[:i]


async def main():
    if not API_KEY:
        raise ValueError("Missing API_KEY in .env file")

    uri = "wss://socket.polygon.io/options"

    while True:
        try:
            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                await ws.send(json.dumps({
                    "action": "auth",
                    "params": API_KEY
                }))

                print("✅ Connected to OPTIONS feed")

                # listen to all option trades first
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": "T.*"
                }))

                print("📡 Listening to options trades...\n")

                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    for event in data:
                        if event.get("ev") == "status":
                            print(f"STATUS: {event}")
                            continue

                        if event.get("ev") == "T":
                            option_symbol = event.get("sym", "")
                            underlying = get_underlying_from_option_symbol(option_symbol)

                            if underlying not in WATCHLIST:
                                continue

                            price = event.get("p", 0)
                            size = event.get("s", 0)

                            # contracts * price * 100
                            premium = price * size * 100

                            # only print bigger options trades
                            if premium >= 25000:
                                print(
                                    f"{underlying} | {option_symbol} | "
                                    f"Price: ${price:.2f} | Contracts: {size} | Premium: ${premium:,.0f}"
                                )

        except websockets.exceptions.ConnectionClosedError as error:
            print(f"⚠️ Options connection closed: {error}")
            print("🔁 Reconnecting options feed in 5 seconds...\n")
            await asyncio.sleep(5)

        except Exception as error:
            print(f"❌ Options error: {error}")
            print("🔁 Retrying options feed in 5 seconds...\n")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())