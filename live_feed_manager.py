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
# GLOBAL SAFETY LOCKS
# prevents duplicate engine starts
# -----------------------------
_engine_lock = threading.Lock()
_engine_started = False


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
# starts ONLY ONCE
# -----------------------------
def start_live_feed_manager():
    global _engine_started

    with _engine_lock:
        if _engine_started:
            print("✅ Live feed manager already running. Skipping duplicate start.")
            return

        print("🚀 Starting live feed manager...")

        # STOCK FEED THREAD
        stock_thread = threading.Thread(
            target=_run_async_task,
            args=(run_stocks_feed(),),
            daemon=True,
            name="stocks-feed-thread",
        )

        # OPTIONS FEED THREAD
        options_thread = threading.Thread(
            target=_run_async_task,
            args=(run_options_feed(),),
            daemon=True,
            name="options-feed-thread",
        )

        # BACKEND REFRESH THREAD
        backend_thread = threading.Thread(
            target=_backend_refresh_loop,
            daemon=True,
            name="backend-refresh-thread",
        )

        # PERSISTENCE THREAD
        persist_thread = threading.Thread(
            target=_persist_loop,
            daemon=True,
            name="persist-thread",
        )

        # START EVERYTHING ONLY ONE TIME
        stock_thread.start()
        options_thread.start()
        backend_thread.start()
        persist_thread.start()

        _engine_started = True

        print("✅ Live feed manager started successfully.")