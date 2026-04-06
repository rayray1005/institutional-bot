from data_store import market_data


def generate_support_resistance_zones(symbol: str):
    data = market_data[symbol]
    candles = list(data.get("candles", []))

    if len(candles) < 5:
        data["support"] = 0.0
        data["resistance"] = 0.0
        data["trading_range"] = {
            "support": 0.0,
            "resistance": 0.0,
            "range_width": 0.0,
        }
        return

    highs = [c["high"] for c in candles[-20:]]
    lows = [c["low"] for c in candles[-20:]]

    support = round(min(lows), 2) if lows else 0.0
    resistance = round(max(highs), 2) if highs else 0.0
    range_width = round(resistance - support, 2) if resistance and support else 0.0

    data["support"] = support
    data["resistance"] = resistance
    data["trading_range"] = {
        "support": support,
        "resistance": resistance,
        "range_width": range_width,
    }


def update_summary_blocks(symbol: str):
    data = market_data[symbol]

    buy_flow = data.get("buy_flow", 0.0)
    sell_flow = data.get("sell_flow", 0.0)
    net_flow = round(buy_flow - sell_flow, 2)
    data["net_flow"] = net_flow

    call_premium = data.get("call_premium", 0.0)
    put_premium = data.get("put_premium", 0.0)

    if call_premium > put_premium * 1.2:
        options_bias = "CALL HEAVY"
    elif put_premium > call_premium * 1.2:
        options_bias = "PUT HEAVY"
    else:
        options_bias = "NEUTRAL"

    data["options_bias"] = options_bias

    signal_summary = data.get("signal_summary", {})
    direction = signal_summary.get("direction", "NEUTRAL")
    confidence = signal_summary.get("confidence", 0)
    reason = signal_summary.get("reason", "Not enough data yet.")

    support = data.get("support", 0.0)
    resistance = data.get("resistance", 0.0)
    spot = data.get("price", 0.0)

    if direction == "BULLISH":
        thesis = f"Bullish flow building. {reason}."
    elif direction == "SLIGHTLY BULLISH":
        thesis = f"Slight bullish lean. {reason}."
    elif direction == "BEARISH":
        thesis = f"Bearish flow building. {reason}."
    elif direction == "SLIGHTLY BEARISH":
        thesis = f"Slight bearish lean. {reason}."
    else:
        thesis = f"Neutral flow. {reason}."

    if spot > 0 and resistance > 0 and support > 0:
        thesis += f" Spot {spot:.2f}, support {support:.2f}, resistance {resistance:.2f}."

    data["daily_summary"] = {
        "institutional_thesis": thesis,
        "confidence_score": confidence,
    }