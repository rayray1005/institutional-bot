import asyncio
import json
import websockets

from config import API_KEY, OPTIONS_WS_URL
from processors.option_processor import process_option_trade

RECONNECT_DELAY_SECONDS = 5
PING_INTERVAL_SECONDS = 20
PING_TIMEOUT_SECONDS = 20
OPTIONS_SUBSCRIPTION = "T.*"


async def handle_options_message(raw_message):
    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError as error:
        print(f"⚠️ Options JSON decode error: {error}")
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
                process_option_trade(event)
            except Exception as error:
                print(f"⚠️ Option trade processing error: {error}")
                print(f"⚠️ Bad option event: {event}")
            continue


async def authenticate_options_socket(ws):
    await ws.send(
        json.dumps(
            {
                "action": "auth",
                "params": API_KEY,
            }
        )
    )


async def subscribe_options_socket(ws):
    await ws.send(
        json.dumps(
            {
                "action": "subscribe",
                "params": OPTIONS_SUBSCRIPTION,
            }
        )
    )


async def run_options_feed():
    if not API_KEY:
        raise ValueError("Missing API_KEY in .env file")

    while True:
        try:
            print("🔌 Connecting to OPTIONS websocket...")

            async with websockets.connect(
                OPTIONS_WS_URL,
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS,
                max_size=None,
            ) as ws:
                await authenticate_options_socket(ws)
                print("✅ Connected to OPTIONS feed")

                await subscribe_options_socket(ws)
                print(f"📡 Listening to options trades: {OPTIONS_SUBSCRIPTION}")

                while True:
                    raw_message = await ws.recv()
                    await handle_options_message(raw_message)

        except websockets.exceptions.ConnectionClosedError as error:
            print(f"⚠️ Options connection closed: {error}")
            print(f"🔁 Reconnecting options feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        except websockets.exceptions.ConnectionClosedOK:
            print("⚠️ Options connection closed normally")
            print(f"🔁 Reconnecting options feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        except Exception as error:
            print(f"⚠️ Options error: {error}")
            print(f"🔁 Retrying options feed in {RECONNECT_DELAY_SECONDS} seconds...")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)