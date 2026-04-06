import asyncio
import json
import websockets

from config import API_KEY, WATCHLIST, STOCKS_WS_URL
from processors.stock_processor import process_stock_trade

RECONNECT_DELAY_SECONDS = 5
PING_INTERVAL_SECONDS = 20
PING_TIMEOUT_SECONDS = 20


async def handle_stock_message(raw_message):
    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError as error:
        print(f"⚠️ Stock JSON decode error: {error}")
        return

    if not isinstance(data, list):
        return

    for event in data:
        if not isinstance(event, dict):
            continue

        event_type = event.get("ev")

        if event_type == "status":
            print(f"STATUS: {event}")
            continue

        if event_type == "T":
            try:
                process_stock_trade(event)
            except Exception as error:
                print(f"⚠️ Stock trade processing error: {error}")
            continue


async def authenticate_stock_socket(ws):
    await ws.send(
        json.dumps(
            {
                "action": "auth",
                "params": API_KEY,
            }
        )
    )


async def subscribe_stock_socket(ws):
    symbols = ",".join([f"T.{symbol}" for symbol in WATCHLIST])

    await ws.send(
        json.dumps(
            {
                "action": "subscribe",
                "params": symbols,
            }
        )
    )


async def run_stocks_feed():
    if not API_KEY:
        raise ValueError("Missing API_KEY in .env file")

    while True:
        try:
            async with websockets.connect(
                STOCKS_WS_URL,
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS,
            ) as ws:
                await authenticate_stock_socket(ws)
                print("✅ Connected to STOCK feed")

                await subscribe_stock_socket(ws)
                print("📡 Listening to stock trades...")

                while True:
                    raw_message = await ws.recv()
                    await handle_stock_message(raw_message)

        except websockets.exceptions.ConnectionClosedError as error:
            print(f"⚠️ Stock connection closed: {error}")
            print(f"🔁 Reconnecting stock feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        except websockets.exceptions.ConnectionClosedOK:
            print("⚠️ Stock connection closed normally")
            print(f"🔁 Reconnecting stock feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        except Exception as error:
            print(f"⚠️ Stock error: {error}")
            print(f"🔁 Retrying stock feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(run_stocks_feed())
    except KeyboardInterrupt:
        print("Stocks feed stopped")