import threading
import asyncio
import time

from feeds.stocks_live import run_stocks_feed
from feeds.options_live import run_options_feed

from data_store import refresh_all_sticky, save_market_state


# -----------------------------
# SETTINGS
# -----------------------------
BACKEND_REFRESH_SECONDS = 1
PERSIST_SECONDS = 15


# -----------------------------
# ASYNC WRAPPER
# -----------------------------
def _run_async_task(coro):
    asyncio.run(coro)


# -----------------------------
# BACKEND REFRESH LOOP
# keeps sticky data alive + synced
# -----------------------------
def _backend_refresh_loop():
    while True:
        try:
            refresh_all_sticky()
        except Exception as e:
            print(f"[Backend Refresh Error] {e}")

        time.sleep(BACKEND_REFRESH_SECONDS)


# -----------------------------
# PERSIST LOOP
# saves market state to disk
# -----------------------------
def _persist_loop():
    while True:
        try:
            save_market_state()
        except Exception as e:
            print(f"[Persistence Error] {e}")

        time.sleep(PERSIST_SECONDS)


# -----------------------------
# MAIN STARTER
# -----------------------------
def start_live_feed_manager():
    # STOCK FEED
    stock_thread = threading.Thread(
        target=_run_async_task,
        args=(run_stocks_feed(),),
        daemon=True,
    )

    # OPTIONS FEED
    options_thread = threading.Thread(
        target=_run_async_task,
        args=(run_options_feed(),),
        daemon=True,
    )

    # BACKEND REFRESH THREAD
    backend_thread = threading.Thread(
        target=_backend_refresh_loop,
        daemon=True,
    )

    # PERSISTENCE THREAD
    persist_thread = threading.Thread(
        target=_persist_loop,
        daemon=True,
    )

    # START EVERYTHING
    stock_thread.start()
    options_thread.start()
    backend_thread.start()
    persist_thread.start()