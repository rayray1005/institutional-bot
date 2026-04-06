import random
import threading
import time
from datetime import datetime, timedelta

from config import WATCHLIST
from data_store import market_data

BASE_PRICES = {
    "SPY": 520,
    "TSLA": 175,
    "INTC": 23,
    "META": 505,
    "NVDA": 900,
    "SNDK": 108,
    "MU": 121,
    "AMZN": 182,
    "PLTR": 24,
    "AAPL": 210,
    "NFLX": 630,
    "IWM": 205,
    "AMD": 165,
}


def classify_options_bias(call_premium, put_premium):
    if call_premium > put_premium * 1.2:
        return "CALL HEAVY"
    if put_premium > call_premium * 1.2:
        return "PUT HEAVY"
    return "NEUTRAL"


def seed_initial_candles(symbol):
    data = market_data[symbol]

    if len(data["candles"]) > 0:
        return

    base = BASE_PRICES.get(symbol, 100.0)
    now = datetime.now() - timedelta(minutes=5 * 120)
    last_close = base

    for i in range(120):
        ts = now + timedelta(minutes=5 * i)

        open_price = last_close
        move = random.uniform(-1.2, 1.2)
        close_price = max(1, open_price + move)
        high_price = max(open_price, close_price) + random.uniform(0.1, 0.8)
        low_price = min(open_price, close_price) - random.uniform(0.1, 0.8)
        volume = random.randint(10_000, 120_000)

        candle = {
            "timestamp": ts,
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
        }
        data["candles"].append(candle)
        last_close = close_price

    data["price"] = round(last_close, 2)
    data["change"] = round(last_close - base, 2)


def generate_support_resistance_zones(symbol):
    data = market_data[symbol]
    candles = list(data["candles"])

    if len(candles) < 20:
        data["support_zones"] = []
        data["resistance_zones"] = []
        return

    recent = candles[-40:]
    recent_lows = [c["low"] for c in recent]
    recent_highs = [c["high"] for c in recent]

    support_low = min(recent_lows)
    resistance_high = max(recent_highs)

    support_zone = {
        "zone_type": "support",
        "low": round(support_low, 2),
        "high": round(support_low + max(0.3, support_low * 0.004), 2),
        "label": "Bullish Order Block",
    }

    resistance_zone = {
        "zone_type": "resist",
        "low": round(resistance_high - max(0.3, resistance_high * 0.004), 2),
        "high": round(resistance_high, 2),
        "label": "Bearish Order Block",
    }

    data["support_zones"] = [support_zone]
    data["resistance_zones"] = [resistance_zone]
    data["support_levels"] = [support_zone["low"], support_zone["high"]]
    data["resistance_levels"] = [resistance_zone["low"], resistance_zone["high"]]

    current_price = data["price"]
    nearest_support = support_zone["high"]
    nearest_resistance = resistance_zone["low"]

    data["trading_range"] = {
        "current_price": round(current_price, 2),
        "support": round(nearest_support, 2),
        "resistance": round(nearest_resistance, 2),
        "range_width": round(nearest_resistance - nearest_support, 2),
    }


def generate_mock_option_print(symbol):
    data = market_data[symbol]
    spot = data["price"]
    now = datetime.now()

    contract = random.choice(["C", "P"])
    dte_bucket = random.choices(
        population=["tier1", "tier2", "other"],
        weights=[0.45, 0.4, 0.15],
        k=1,
    )[0]

    if dte_bucket == "tier1":
        dte = random.randint(0, 3)
    elif dte_bucket == "tier2":
        dte = random.randint(3, 14)
    else:
        dte = random.randint(15, 45)

    expiry = (now + timedelta(days=dte)).strftime("%Y-%m-%d")

    strike_step = max(1, round(spot * 0.005))
    strike_offset_steps = random.choice([-3, -2, -1, 0, 1, 2, 3])
    strike = round(round((spot + strike_offset_steps * strike_step)))

    option_price = round(random.uniform(0.15, 4.50), 2)
    contracts = random.randint(20, 800)
    premium = round(option_price * contracts * 100, 2)

    is_atm = abs(strike - spot) / max(spot, 1) <= 0.02

    if contract == "C":
        side = random.choice(["call_buy", "call_sell"])
        delta_sign = 1 if side == "call_buy" else -1
    else:
        side = random.choice(["put_buy", "put_sell"])
        delta_sign = -1 if side == "put_buy" else 1

    delta_exposure = round(premium * random.uniform(2.0, 12.0) * delta_sign, 2)

    option_symbol = f"{symbol}_{expiry}_{int(strike)}{contract}"

    tags = [
        "Large Block",
        "ATM Sweep" if is_atm else "Standard Institutional",
        "Momentum Call" if contract == "C" else "Protective Put",
    ]
    tag = random.choice(tags)

    return {
        "timestamp": now,
        "option_symbol": option_symbol,
        "contract": contract,
        "strike": float(strike),
        "expiry": expiry,
        "dte": dte,
        "price": option_price,
        "contracts": contracts,
        "premium": premium,
        "side": side,
        "delta_exposure": delta_exposure,
        "is_atm": is_atm,
        "tier": dte_bucket,
        "tag": tag,
    }


def update_summary_blocks(symbol):
    data = market_data[symbol]
    prints = list(data["option_prints"])
    spot = data["price"]

    tier1 = [p for p in prints if p["tier"] == "tier1"]
    tier2 = [p for p in prints if p["tier"] == "tier2"]
    atm = [p for p in prints if p["is_atm"]]

    def summarize_bucket(bucket):
        call_bucket = [p for p in bucket if p["contract"] == "C"]
        put_bucket = [p for p in bucket if p["contract"] == "P"]

        call_notional = sum(p["premium"] for p in call_bucket)
        put_notional = sum(p["premium"] for p in put_bucket)
        call_real_delta = sum(p["delta_exposure"] for p in call_bucket)
        put_real_delta = sum(abs(p["delta_exposure"]) for p in put_bucket)

        call_prints = len(call_bucket)
        put_prints = len(put_bucket)

        avg_dte_calls = sum(p["dte"] for p in call_bucket) / call_prints if call_prints else 0.0
        avg_dte_puts = sum(p["dte"] for p in put_bucket) / put_prints if put_prints else 0.0
        avg_size_calls = sum(p["contracts"] for p in call_bucket) / call_prints if call_prints else 0.0
        avg_size_puts = sum(p["contracts"] for p in put_bucket) / put_prints if put_prints else 0.0

        notional_cp_ratio = round(call_notional / put_notional, 2) if put_notional > 0 else 0.0
        real_delta_cp_ratio = round(abs(call_real_delta) / put_real_delta, 2) if put_real_delta > 0 else 0.0
        net_delta_exposure = round(sum(p["delta_exposure"] for p in bucket), 2)

        if net_delta_exposure > 250_000:
            interpretation = "Bullish institutional positioning."
        elif net_delta_exposure < -250_000:
            interpretation = "Bearish institutional positioning."
        else:
            interpretation = "Balanced / neutral institutional positioning."

        return {
            "call_notional": round(call_notional, 2),
            "put_notional": round(put_notional, 2),
            "call_prints": call_prints,
            "put_prints": put_prints,
            "call_real_delta": round(call_real_delta, 2),
            "put_real_delta": round(put_real_delta, 2),
            "avg_dte_calls": round(avg_dte_calls, 2),
            "avg_dte_puts": round(avg_dte_puts, 2),
            "avg_size_calls": round(avg_size_calls, 2),
            "avg_size_puts": round(avg_size_puts, 2),
            "notional_cp_ratio": notional_cp_ratio,
            "real_delta_cp_ratio": real_delta_cp_ratio,
            "net_delta_exposure": net_delta_exposure,
            "interpretation": interpretation,
        }

    data["tier1"] = summarize_bucket(tier1)
    data["tier2"] = summarize_bucket(tier2)

    atm_summary = summarize_bucket(atm)
    atm_low = round(spot * 0.98, 2)
    atm_high = round(spot * 1.02, 2)
    atm_summary["spot_price"] = round(spot, 2)
    atm_summary["atm_low"] = atm_low
    atm_summary["atm_high"] = atm_high
    data["atm_flow"] = atm_summary

    sorted_tier1 = sorted(tier1, key=lambda x: x["premium"], reverse=True)[:5]
    sorted_tier2 = sorted(tier2, key=lambda x: x["premium"], reverse=True)[:5]
    data["top_prints"] = {
        "tier1": sorted_tier1,
        "tier2": sorted_tier2,
    }

    concentration = sorted(prints, key=lambda x: abs(x["delta_exposure"]), reverse=True)[:10]
    data["real_delta_concentration"] = concentration

    total_notional = sum(p["premium"] for p in prints)
    total_delta = sum(p["delta_exposure"] for p in prints)

    tier1_label = "BULLISH" if data["tier1"]["net_delta_exposure"] > 0 else "BEARISH" if data["tier1"]["net_delta_exposure"] < 0 else "NEUTRAL"
    tier2_label = "BULLISH" if data["tier2"]["net_delta_exposure"] > 0 else "BEARISH" if data["tier2"]["net_delta_exposure"] < 0 else "NEUTRAL"
    atm_label = "BULLISH" if data["atm_flow"]["net_delta_exposure"] > 0 else "BEARISH" if data["atm_flow"]["net_delta_exposure"] < 0 else "NEUTRAL"

    confidence = 50
    if tier1_label == "BULLISH":
        confidence += 15
    elif tier1_label == "BEARISH":
        confidence += 15

    if atm_label == tier1_label and atm_label != "NEUTRAL":
        confidence += 10

    confidence = min(confidence, 100)

    thesis = (
        f"Tier-1: {tier1_label}, Tier-2: {tier2_label}, ATM: {atm_label}. "
        f"Trade with {('bullish' if total_delta > 0 else 'bearish' if total_delta < 0 else 'neutral')} bias."
    )

    data["daily_summary"] = {
        "total_institutional_trades": len(prints),
        "total_notional": round(total_notional, 2),
        "net_delta_exposure": round(total_delta, 2),
        "equity_flow_label": "BULLISH" if data["net_flow"] > 0 else "BEARISH" if data["net_flow"] < 0 else "NEUTRAL",
        "tier1_label": tier1_label,
        "tier2_label": tier2_label,
        "atm_label": atm_label,
        "institutional_thesis": thesis,
        "confidence_score": confidence,
    }

    data["daily_total"] = {
        "spot_price": round(spot, 2),
        "buy_volume": round(data["buy_flow"], 2),
        "sell_volume": round(data["sell_flow"], 2),
        "net_flow": round(data["net_flow"], 2),
        "trades": data["trade_count"],
        "avg_size": round(data["avg_trade_size"], 2),
        "classification_rate": round(data["classification_rate"], 2),
    }


def update_symbol(symbol):
    data = market_data[symbol]

    if data["price"] == 0:
        data["price"] = BASE_PRICES.get(symbol, 100.0)

    # ---------------------------------
    # PRICE UPDATE
    # ---------------------------------
    old_price = data["price"]
    move = random.uniform(-0.8, 0.8)
    new_price = round(max(1, old_price + move), 2)

    data["price"] = new_price
    data["change"] = round(data["change"] + move, 2)

    # ---------------------------------
    # EQUITY FLOW UPDATE
    # ---------------------------------
    buy = random.uniform(1_000, 50_000)
    sell = random.uniform(1_000, 50_000)

    data["buy_flow"] += buy
    data["sell_flow"] += sell
    data["net_flow"] = round(data["buy_flow"] - data["sell_flow"], 2)

    trade_size = random.randint(50, 2000)
    data["trade_count"] += 1
    running_count = data["trade_count"]

    if running_count == 1:
        data["avg_trade_size"] = trade_size
    else:
        data["avg_trade_size"] = (
            (data["avg_trade_size"] * (running_count - 1)) + trade_size
        ) / running_count

    data["classification_rate"] = round(random.uniform(65, 98), 2)

    if random.random() > 0.8:
        data["big_trades"] += 1
        data["last_trade"] = f"BIG {symbol} @ ${new_price}"

    # ---------------------------------
    # BASIC OPTIONS TOTALS
    # ---------------------------------
    call = random.uniform(0, 20_000)
    put = random.uniform(0, 20_000)

    data["call_premium"] += call
    data["put_premium"] += put

    if call > 5_000:
        data["call_trades"] += 1
    if put > 5_000:
        data["put_trades"] += 1

    data["options_bias"] = classify_options_bias(data["call_premium"], data["put_premium"])

    # ---------------------------------
    # UPDATE CANDLE
    # ---------------------------------
    last_candle = data["candles"][-1] if len(data["candles"]) > 0 else None
    now = datetime.now()

    if last_candle:
        candle_age = (now - last_candle["timestamp"]).total_seconds()
    else:
        candle_age = 999999

    if candle_age >= 5:
        open_price = old_price
        close_price = new_price
        high_price = max(open_price, close_price) + random.uniform(0.05, 0.5)
        low_price = min(open_price, close_price) - random.uniform(0.05, 0.5)
        volume = random.randint(10_000, 100_000)

        data["candles"].append(
            {
                "timestamp": now,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
            }
        )
    else:
        last_candle["high"] = round(max(last_candle["high"], new_price), 2)
        last_candle["low"] = round(min(last_candle["low"], new_price), 2)
        last_candle["close"] = round(new_price, 2)
        last_candle["volume"] += random.randint(500, 3000)

    # ---------------------------------
    # OPTION PRINTS
    # ---------------------------------
    if random.random() > 0.35:
        new_print = generate_mock_option_print(symbol)
        data["option_prints"].append(new_print)

    # ---------------------------------
    # LEVELS / ZONES / SUMMARY
    # ---------------------------------
    generate_support_resistance_zones(symbol)
    update_summary_blocks(symbol)


def run_mock_feed():
    for symbol in WATCHLIST:
        seed_initial_candles(symbol)
        generate_support_resistance_zones(symbol)

    while True:
        for symbol in WATCHLIST:
            update_symbol(symbol)
        time.sleep(1)


def start_mock_feed():
    thread = threading.Thread(target=run_mock_feed, daemon=True)
    thread.start()