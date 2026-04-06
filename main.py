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

async def main():
    if not API_KEY:
        raise ValueError("Missing API_KEY in .env file")

    uri = "wss://socket.polygon.io/stocks"
    symbols = ",".join([f"T.{symbol}" for symbol in WATCHLIST])

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

                print("✅ Connected to STOCK feed")

                await ws.send(json.dumps({
                    "action": "subscribe",
                    "params": symbols
                }))

                print("📡 Listening to stock trades...\n")

                while True:
                    message = await ws.recv()
                    data = json.loads(message)

                    for event in data:
                        if event.get("ev") == "status":
                            print(f"STATUS: {event}")
                            continue

                        if event.get("ev") == "T":
                            symbol = event.get("sym", "")
                            price = event.get("p", 0)
                            size = event.get("s", 0)

                            # only show bigger prints
                            if size >= 500:
                                print(f"{symbol} | ${price} | Size: {size}")

        except websockets.exceptions.ConnectionClosedError as error:
            print(f"⚠️ Stock connection closed: {error}")
            print("🔁 Reconnecting stock feed in 5 seconds...\n")
            await asyncio.sleep(5)

        except Exception as error:
            print(f"❌ Stock error: {error}")
            print("🔁 Retrying stock feed in 5 seconds...\n")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())