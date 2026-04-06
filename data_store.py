from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
import threading
import json
import os

from config import WATCHLIST

MAX_BIG_TRADES = 200
MAX_CANDLES = 500
MAX_OPTION_PRINTS = 200

# -----------------------------
# GLOBAL THREAD LOCK
# protects shared live state
# -----------------------------
data_lock = threading.RLock()


def create_symbol_state():
    return {
        # -----------------------------
        # STOCK STATE
        # -----------------------------
        "price": 0.0,
        "change": 0.0,
        "buy_flow": 0.0,
        "sell_flow": 0.0,
        "net_flow": 0.0,
        "big_trades": deque(maxlen=MAX_BIG_TRADES),
        "last_trade": "",
        "trade_count": 0,
        "avg_trade_size": 0.0,
        "classification_rate": 0.0,
        "last_stock_update": "",
        "last_option_update": "",
        "stock_events": 0,
        "option_events": 0,

        # -----------------------------
        # PRICE / CANDLES / LEVELS
        # -----------------------------
        "candles": deque(maxlen=MAX_CANDLES),
        "support": 0.0,
        "resistance": 0.0,

        # -----------------------------
        # OPTIONS SUMMARY
        # -----------------------------
        "call_premium": 0.0,
        "put_premium": 0.0,
        "call_trades": 0,
        "put_trades": 0,
        "options_bias": "NEUTRAL",
        "option_prints": deque(maxlen=MAX_OPTION_PRINTS),

        # -----------------------------
        # STRIKE-LEVEL OPTIONS INTEL
        # -----------------------------
        "call_premium_by_strike": {},
        "put_premium_by_strike": {},
        "total_premium_by_strike": {},
        "call_volume_by_strike": {},
        "put_volume_by_strike": {},
        "strike_last_update": {},

        # -----------------------------
        # TIER BREAKDOWN
        # -----------------------------
        "tier_stats": {
            "tier1": {"count": 0, "premium": 0.0},
            "tier2": {"count": 0, "premium": 0.0},
            "other": {"count": 0, "premium": 0.0},
        },

        # -----------------------------
        # LIVE INSTITUTIONAL PANELS
        # -----------------------------
        "top_option_trades": [],
        "delta_walls": [],
        "gamma_zones": [],
        "signal_summary": {
            "direction": "NEUTRAL",
            "confidence": 0,
            "reason": "Not enough data yet.",
        },

        # -----------------------------
        # DASHBOARD SUMMARY
        # -----------------------------
        "daily_summary": {
            "institutional_thesis": "Not enough data yet.",
            "confidence_score": 0,
        },
        "trading_range": {
            "support": 0.0,
            "resistance": 0.0,
            "range_width": 0.0,
        },
    }


# -----------------------------
# RAW LIVE STATE
# -----------------------------
market_data = {symbol: create_symbol_state() for symbol in WATCHLIST}

# -----------------------------
# STICKY LAST-GOOD DISPLAY STATE
# shared across dashboard / chart / flow detail
# -----------------------------
sticky_market_data = {symbol: {} for symbol in WATCHLIST}
last_backend_refresh_utc = None

NUMERIC_FIELDS_TO_STICK = [
    "price",
    "change",
    "buy_flow",
    "sell_flow",
    "net_flow",
    "call_premium",
    "put_premium",
    "call_trades",
    "put_trades",
    "trade_count",
    "avg_trade_size",
    "classification_rate",
    "support",
    "resistance",
    "stock_events",
    "option_events",
]

LIST_FIELDS_TO_STICK = [
    "top_option_trades",
    "delta_walls",
    "gamma_zones",
]

TEXT_FIELDS_TO_STICK = [
    "options_bias",
    "last_trade",
    "last_stock_update",
    "last_option_update",
]

DICT_FIELDS_TO_STICK = [
    "call_premium_by_strike",
    "put_premium_by_strike",
    "total_premium_by_strike",
    "call_volume_by_strike",
    "put_volume_by_strike",
    "strike_last_update",
    "tier_stats",
    "signal_summary",
    "daily_summary",
    "trading_range",
]

DEQUE_FIELDS_TO_STICK = [
    "big_trades",
    "candles",
    "option_prints",
]

STATE_FILE = "market_state.json"


def _is_valid_number(value):
    return isinstance(value, (int, float)) and value is not None


def _clone_value(value):
    if isinstance(value, deque):
        return deque(list(value), maxlen=value.maxlen)
    if isinstance(value, dict):
        return {k: _clone_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clone_value(v) for v in value]
    return deepcopy(value)


def _empty_value_for_field(field):
    if field in TEXT_FIELDS_TO_STICK:
        return ""
    if field in LIST_FIELDS_TO_STICK:
        return []
    if field in DICT_FIELDS_TO_STICK:
        return {}
    if field == "big_trades":
        return deque(maxlen=MAX_BIG_TRADES)
    if field == "candles":
        return deque(maxlen=MAX_CANDLES)
    if field == "option_prints":
        return deque(maxlen=MAX_OPTION_PRINTS)
    if field in NUMERIC_FIELDS_TO_STICK:
        return 0.0
    return None


def _prefer_live_over_cached(field, live_val, cached_val):
    # -----------------------------
    # TEXT
    # -----------------------------
    if field in TEXT_FIELDS_TO_STICK:
        if isinstance(live_val, str) and live_val.strip():
            return live_val
        if cached_val is not None:
            return cached_val
        return _empty_value_for_field(field)

    # -----------------------------
    # LISTS
    # -----------------------------
    if field in LIST_FIELDS_TO_STICK:
        if isinstance(live_val, list) and len(live_val) > 0:
            return deepcopy(live_val)
        if cached_val is not None:
            return deepcopy(cached_val)
        return _empty_value_for_field(field)

    # -----------------------------
    # DICTS
    # -----------------------------
    if field in DICT_FIELDS_TO_STICK:
        if isinstance(live_val, dict) and len(live_val) > 0:
            return deepcopy(live_val)
        if cached_val is not None:
            return deepcopy(cached_val)
        return _empty_value_for_field(field)

    # -----------------------------
    # DEQUES
    # -----------------------------
    if field in DEQUE_FIELDS_TO_STICK:
        if isinstance(live_val, deque) and len(live_val) > 0:
            return deque(list(live_val), maxlen=live_val.maxlen)

        if isinstance(cached_val, deque):
            return deque(list(cached_val), maxlen=cached_val.maxlen)

        return _empty_value_for_field(field)

    # -----------------------------
    # NUMBERS
    # keep last good value instead of useless zero
    # -----------------------------
    if field in NUMERIC_FIELDS_TO_STICK:
        if _is_valid_number(live_val):
            if live_val != 0:
                return live_val
            if cached_val is None:
                return live_val
            return cached_val

        if cached_val is not None:
            return cached_val

        return _empty_value_for_field(field)

    # -----------------------------
    # FALLBACK
    # -----------------------------
    if live_val not in [None, "", [], {}]:
        return _clone_value(live_val)

    if cached_val is not None:
        return _clone_value(cached_val)

    return None


def _safe_symbol_snapshot_under_lock(symbol):
    """
    Clone one symbol's state safely while holding the lock.
    Avoids iterating live shared deques outside the lock.
    """
    if symbol not in market_data:
        market_data[symbol] = create_symbol_state()

    src = market_data[symbol]
    out = {}

    for key, value in src.items():
        if isinstance(value, deque):
            out[key] = deque(list(value), maxlen=value.maxlen)
        elif isinstance(value, dict):
            out[key] = deepcopy(value)
        elif isinstance(value, list):
            out[key] = deepcopy(value)
        else:
            out[key] = deepcopy(value)

    return out


def build_sticky_snapshot(symbol):
    with data_lock:
        live = _safe_symbol_snapshot_under_lock(symbol)
        cached = deepcopy(sticky_market_data.get(symbol, {}))

    merged = {}

    all_fields = (
        NUMERIC_FIELDS_TO_STICK
        + LIST_FIELDS_TO_STICK
        + TEXT_FIELDS_TO_STICK
        + DICT_FIELDS_TO_STICK
        + DEQUE_FIELDS_TO_STICK
    )

    for field in all_fields:
        merged[field] = _prefer_live_over_cached(
            field=field,
            live_val=live.get(field),
            cached_val=cached.get(field),
        )

    # -----------------------------
    # SAFETY DEFAULTS
    # -----------------------------
    merged["options_bias"] = merged.get("options_bias") or "NEUTRAL"

    merged["signal_summary"] = merged.get("signal_summary") or {
        "direction": "NEUTRAL",
        "confidence": 0,
        "reason": "Not enough data yet.",
    }

    merged["daily_summary"] = merged.get("daily_summary") or {
        "institutional_thesis": "Not enough data yet.",
        "confidence_score": 0,
    }

    merged["trading_range"] = merged.get("trading_range") or {
        "support": 0.0,
        "resistance": 0.0,
        "range_width": 0.0,
    }

    merged["tier_stats"] = merged.get("tier_stats") or {
        "tier1": {"count": 0, "premium": 0.0},
        "tier2": {"count": 0, "premium": 0.0},
        "other": {"count": 0, "premium": 0.0},
    }

    if not isinstance(merged.get("big_trades"), deque):
        merged["big_trades"] = deque(merged.get("big_trades", []), maxlen=MAX_BIG_TRADES)

    if not isinstance(merged.get("candles"), deque):
        merged["candles"] = deque(merged.get("candles", []), maxlen=MAX_CANDLES)

    if not isinstance(merged.get("option_prints"), deque):
        merged["option_prints"] = deque(merged.get("option_prints", []), maxlen=MAX_OPTION_PRINTS)

    return merged


def refresh_symbol_sticky(symbol):
    global last_backend_refresh_utc

    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

    snapshot = build_sticky_snapshot(symbol)

    with data_lock:
        sticky_market_data[symbol] = snapshot
        last_backend_refresh_utc = datetime.now(timezone.utc)


def refresh_all_sticky():
    global last_backend_refresh_utc

    snapshots = {}

    with data_lock:
        for symbol in WATCHLIST:
            snapshots[symbol] = _safe_symbol_snapshot_under_lock(symbol)

        cached_copy = deepcopy(sticky_market_data)

    for symbol in WATCHLIST:
        live = snapshots.get(symbol, {})
        cached = cached_copy.get(symbol, {})
        merged = {}

        all_fields = (
            NUMERIC_FIELDS_TO_STICK
            + LIST_FIELDS_TO_STICK
            + TEXT_FIELDS_TO_STICK
            + DICT_FIELDS_TO_STICK
            + DEQUE_FIELDS_TO_STICK
        )

        for field in all_fields:
            merged[field] = _prefer_live_over_cached(
                field=field,
                live_val=live.get(field),
                cached_val=cached.get(field),
            )

        merged["options_bias"] = merged.get("options_bias") or "NEUTRAL"

        merged["signal_summary"] = merged.get("signal_summary") or {
            "direction": "NEUTRAL",
            "confidence": 0,
            "reason": "Not enough data yet.",
        }

        merged["daily_summary"] = merged.get("daily_summary") or {
            "institutional_thesis": "Not enough data yet.",
            "confidence_score": 0,
        }

        merged["trading_range"] = merged.get("trading_range") or {
            "support": 0.0,
            "resistance": 0.0,
            "range_width": 0.0,
        }

        merged["tier_stats"] = merged.get("tier_stats") or {
            "tier1": {"count": 0, "premium": 0.0},
            "tier2": {"count": 0, "premium": 0.0},
            "other": {"count": 0, "premium": 0.0},
        }

        if not isinstance(merged.get("big_trades"), deque):
            merged["big_trades"] = deque(merged.get("big_trades", []), maxlen=MAX_BIG_TRADES)

        if not isinstance(merged.get("candles"), deque):
            merged["candles"] = deque(merged.get("candles", []), maxlen=MAX_CANDLES)

        if not isinstance(merged.get("option_prints"), deque):
            merged["option_prints"] = deque(merged.get("option_prints", []), maxlen=MAX_OPTION_PRINTS)

        snapshots[symbol] = merged

    with data_lock:
        for symbol, snapshot in snapshots.items():
            sticky_market_data[symbol] = snapshot
        last_backend_refresh_utc = datetime.now(timezone.utc)


def get_live_data(symbol):
    """
    WARNING:
    This returns the live mutable object.
    Keep this only for compatibility.
    Do not mutate nested shared structures outside data_lock.
    """
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()
        return market_data[symbol]


def get_live_snapshot(symbol):
    with data_lock:
        return _safe_symbol_snapshot_under_lock(symbol)


def get_display_data(symbol):
    with data_lock:
        if symbol not in sticky_market_data:
            sticky_market_data[symbol] = {}
        needs_refresh = not sticky_market_data[symbol]

    if needs_refresh:
        refresh_symbol_sticky(symbol)

    with data_lock:
        return deepcopy(sticky_market_data[symbol])


def get_all_display_data():
    refresh_all_sticky()
    with data_lock:
        return deepcopy(sticky_market_data)


def update_symbol_data(symbol, updates):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        for key, value in updates.items():
            market_data[symbol][key] = value

    refresh_symbol_sticky(symbol)


# -----------------------------
# SAFE MUTATION HELPERS
# Use these from processors instead of mutating
# nested shared structures directly.
# -----------------------------
def set_field(symbol, field, value):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()
        market_data[symbol][field] = value


def increment_field(symbol, field, amount):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        current = market_data[symbol].get(field, 0)
        if isinstance(current, (int, float)):
            market_data[symbol][field] = current + amount
        else:
            market_data[symbol][field] = amount


def append_to_deque(symbol, field, item):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        target = market_data[symbol].get(field)
        if isinstance(target, deque):
            target.append(item)


def extend_deque(symbol, field, items):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        target = market_data[symbol].get(field)
        if isinstance(target, deque):
            for item in items:
                target.append(item)


def set_dict_value(symbol, field, key, value):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        if field not in market_data[symbol] or not isinstance(market_data[symbol][field], dict):
            market_data[symbol][field] = {}

        market_data[symbol][field][key] = value


def increment_dict_value(symbol, field, key, amount):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        if field not in market_data[symbol] or not isinstance(market_data[symbol][field], dict):
            market_data[symbol][field] = {}

        current = market_data[symbol][field].get(key, 0)
        if isinstance(current, (int, float)):
            market_data[symbol][field][key] = current + amount
        else:
            market_data[symbol][field][key] = amount


def replace_list(symbol, field, items):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()
        market_data[symbol][field] = list(items)


def clear_field(symbol, field):
    with data_lock:
        if symbol not in market_data:
            market_data[symbol] = create_symbol_state()

        if field not in market_data[symbol]:
            return

        current = market_data[symbol][field]

        if isinstance(current, deque):
            current.clear()
        elif isinstance(current, list):
            market_data[symbol][field] = []
        elif isinstance(current, dict):
            market_data[symbol][field] = {}
        elif isinstance(current, str):
            market_data[symbol][field] = ""
        elif isinstance(current, (int, float)):
            market_data[symbol][field] = 0
        else:
            market_data[symbol][field] = None


# =============================
# PERSISTENCE LAYER
# =============================

def _serialize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, deque):
        return [_serialize_value(v) for v in list(value)]

    if isinstance(value, list):
        return [_serialize_value(v) for v in list(value)]

    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in dict(value).items()}

    return value


def save_market_state(filepath=STATE_FILE):
    try:
        with data_lock:
            safe_market_copy = {
                symbol: _safe_symbol_snapshot_under_lock(symbol)
                for symbol in WATCHLIST
            }

        payload = {}

        for symbol, data in safe_market_copy.items():
            payload[symbol] = _serialize_value(data)

        with open(filepath, "w") as f:
            json.dump(payload, f)

    except Exception as e:
        print(f"[SAVE ERROR] {e}")


def _restore_value(value):
    if isinstance(value, list):
        return [_restore_value(v) for v in value]

    if isinstance(value, dict):
        return {k: _restore_value(v) for k, v in value.items()}

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return value

    return value


def load_market_state(filepath=STATE_FILE):
    if not os.path.exists(filepath):
        return

    try:
        with open(filepath, "r") as f:
            payload = json.load(f)

        restored_market = {}

        for symbol, saved_data in payload.items():
            if symbol not in market_data:
                continue

            restored = create_symbol_state()

            for key, value in saved_data.items():
                if key in ["big_trades", "candles", "option_prints"]:
                    restored[key] = deque(
                        [_restore_value(v) for v in value],
                        maxlen=restored[key].maxlen,
                    )
                else:
                    restored[key] = _restore_value(value)

            restored_market[symbol] = restored

        with data_lock:
            for symbol, restored in restored_market.items():
                market_data[symbol] = restored

        refresh_all_sticky()

        print("✅ Market state loaded")

    except Exception as e:
        print(f"[LOAD ERROR] {e}")


# AUTO LOAD ON START
load_market_state()