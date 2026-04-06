import os
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

# Massive WebSocket endpoints
STOCKS_WS_URL = "wss://socket.massive.com/stocks"
OPTIONS_WS_URL = "wss://socket.massive.com/options"

# App behavior
APP_REFRESH_SECONDS = 1
DATA_MODE = "live"   # change to "mock" if needed

# Thresholds
LARGE_STOCK_PRINT_SIZE = 500
LARGE_OPTION_PREMIUM = 25000

# Flow logic
ATM_PCT_BAND = 0.02   # ±2% of price = ATM
TIER1_MAX_DTE = 3     # urgent flow
TIER2_MAX_DTE = 14    # patient flow