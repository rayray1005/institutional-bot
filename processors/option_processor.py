from datetime import datetime

from config import ATM_PCT_BAND, TIER1_MAX_DTE, TIER2_MAX_DTE
from data_store import market_data
from analytics.summary_utils import update_summary_blocks, generate_support_resistance_zones


TOP_OPTION_TRADES_LIMIT = 25
DELTA_WALL_COUNT = 5
GAMMA_ZONE_COUNT = 3


def process_option_trade(event: dict):
    option_symbol = event.get("sym") or event.get("symbol") or ""
    underlying = event.get("underlying") or _extract_underlying(option_symbol)

    if not underlying or underlying not in market_data:
        return

    price = float(event.get("p") or event.get("price") or 0)
    contracts = int(event.get("s") or event.get("size") or 0)

    if price <= 0 or contracts <= 0:
        return

    data = market_data[underlying]
    now = datetime.now()

    data["option_events"] += 1
    data["last_option_update"] = now.strftime("%H:%M:%S")

    strike = float(event.get("strike") or 0)
    expiry = event.get("expiry") or ""
    contract_type = event.get("contract_type") or _extract_contract_type(option_symbol)
    spot = float(data.get("price") or 0)

    premium = round(price * contracts * 100, 2)
    dte = _calc_dte(expiry)
    is_atm = abs(strike - spot) / spot <= ATM_PCT_BAND if spot > 0 and strike > 0 else False

    if dte <= TIER1_MAX_DTE:
        tier = "tier1"
    elif dte <= TIER2_MAX_DTE:
        tier = "tier2"
    else:
        tier = "other"

    approx_delta = _approximate_delta(contract_type, strike, spot, dte, is_atm)
    delta_exposure = round(premium * approx_delta, 2)

    print_data = {
        "timestamp": now,
        "option_symbol": option_symbol,
        "underlying": underlying,
        "contract": contract_type,
        "strike": strike,
        "expiry": expiry,
        "dte": dte,
        "price": price,
        "contracts": contracts,
        "premium": premium,
        "side": "unknown",
        "delta_exposure": delta_exposure,
        "approx_delta": round(approx_delta, 4),
        "is_atm": is_atm,
        "tier": tier,
        "tag": _build_print_tag(contract_type, tier, is_atm, premium),
    }

    data["option_prints"].append(print_data)

    _update_option_side_totals(data, contract_type, premium)
    _update_strike_maps(data, strike, contract_type, premium, contracts, now)
    _update_tier_stats(data, tier, premium)
    _update_top_option_trades(data, print_data)
    _update_delta_walls(data, spot)
    _update_gamma_zones(data, spot)
    _update_signal_summary(data, spot)

    generate_support_resistance_zones(underlying)
    update_summary_blocks(underlying)


def _update_option_side_totals(data: dict, contract_type: str, premium: float):
    if contract_type == "C":
        data["call_premium"] += premium
        data["call_trades"] += 1
    else:
        data["put_premium"] += premium
        data["put_trades"] += 1


def _update_strike_maps(
    data: dict,
    strike: float,
    contract_type: str,
    premium: float,
    contracts: int,
    now: datetime,
):
    if strike <= 0:
        return

    strike_key = round(strike, 2)

    if contract_type == "C":
        data["call_premium_by_strike"][strike_key] = (
            data["call_premium_by_strike"].get(strike_key, 0.0) + premium
        )
        data["call_volume_by_strike"][strike_key] = (
            data["call_volume_by_strike"].get(strike_key, 0) + contracts
        )
    else:
        data["put_premium_by_strike"][strike_key] = (
            data["put_premium_by_strike"].get(strike_key, 0.0) + premium
        )
        data["put_volume_by_strike"][strike_key] = (
            data["put_volume_by_strike"].get(strike_key, 0) + contracts
        )

    data["total_premium_by_strike"][strike_key] = (
        data["total_premium_by_strike"].get(strike_key, 0.0) + premium
    )
    data["strike_last_update"][strike_key] = now.strftime("%H:%M:%S")


def _update_tier_stats(data: dict, tier: str, premium: float):
    if tier not in data["tier_stats"]:
        return

    data["tier_stats"][tier]["count"] += 1
    data["tier_stats"][tier]["premium"] += premium


def _update_top_option_trades(data: dict, print_data: dict):
    top_trades = data["top_option_trades"]
    top_trades.append(print_data.copy())
    top_trades.sort(key=lambda x: x.get("premium", 0.0), reverse=True)

    if len(top_trades) > TOP_OPTION_TRADES_LIMIT:
        del top_trades[TOP_OPTION_TRADES_LIMIT:]


def _update_delta_walls(data: dict, spot: float):
    total_map = data.get("total_premium_by_strike", {})
    call_map = data.get("call_premium_by_strike", {})
    put_map = data.get("put_premium_by_strike", {})

    if not total_map:
        data["delta_walls"] = []
        return

    rows = []
    for strike, total_premium in total_map.items():
        call_premium = call_map.get(strike, 0.0)
        put_premium = put_map.get(strike, 0.0)

        if call_premium > put_premium * 1.15:
            bias = "CALL WALL"
        elif put_premium > call_premium * 1.15:
            bias = "PUT WALL"
        else:
            bias = "MIXED WALL"

        distance = round(strike - spot, 2) if spot > 0 else 0.0
        imbalance = abs(call_premium - put_premium)

        rows.append(
            {
                "strike": strike,
                "total_premium": round(total_premium, 2),
                "call_premium": round(call_premium, 2),
                "put_premium": round(put_premium, 2),
                "distance_from_spot": distance,
                "imbalance": round(imbalance, 2),
                "bias": bias,
            }
        )

    if spot > 0:
        rows.sort(
            key=lambda x: (
                abs(x["distance_from_spot"]),
                -x["total_premium"],
                -x["imbalance"],
            )
        )
    else:
        rows.sort(
            key=lambda x: (
                -x["total_premium"],
                -x["imbalance"],
            )
        )

    data["delta_walls"] = rows[:DELTA_WALL_COUNT]


def _update_gamma_zones(data: dict, spot: float):
    total_map = data.get("total_premium_by_strike", {})
    call_vol_map = data.get("call_volume_by_strike", {})
    put_vol_map = data.get("put_volume_by_strike", {})

    if not total_map:
        data["gamma_zones"] = []
        return

    rows = []
    for strike, total_premium in total_map.items():
        call_volume = call_vol_map.get(strike, 0)
        put_volume = put_vol_map.get(strike, 0)
        total_volume = call_volume + put_volume
        distance = round(strike - spot, 2) if spot > 0 else 0.0

        distance_boost = (1 / (abs(distance) + 1)) * 10000 if spot > 0 else 0.0
        zone_strength = round(
            (total_premium * 0.7) +
            (total_volume * 20) +
            distance_boost,
            2,
        )

        rows.append(
            {
                "strike": strike,
                "total_premium": round(total_premium, 2),
                "total_volume": total_volume,
                "distance_from_spot": distance,
                "zone_strength": zone_strength,
            }
        )

    if spot > 0:
        rows.sort(key=lambda x: (abs(x["distance_from_spot"]), -x["zone_strength"]))
    else:
        rows.sort(key=lambda x: x["zone_strength"], reverse=True)

    data["gamma_zones"] = rows[:GAMMA_ZONE_COUNT]


def _update_signal_summary(data: dict, spot: float):
    call_premium = data.get("call_premium", 0.0)
    put_premium = data.get("put_premium", 0.0)
    net_flow = data.get("net_flow", 0.0)
    delta_walls = data.get("delta_walls", [])
    option_prints = list(data.get("option_prints", []))
    tier_stats = data.get("tier_stats", {})

    score = 0
    reasons = []

    if call_premium > put_premium * 1.2:
        score += 2
        reasons.append("calls leading puts")
    elif put_premium > call_premium * 1.2:
        score -= 2
        reasons.append("puts leading calls")

    if net_flow > 0:
        score += 1
        reasons.append("stock flow positive")
    elif net_flow < 0:
        score -= 1
        reasons.append("stock flow negative")

    tier1_premium = float(tier_stats.get("tier1", {}).get("premium", 0.0) or 0.0)
    tier2_premium = float(tier_stats.get("tier2", {}).get("premium", 0.0) or 0.0)

    if tier1_premium > tier2_premium * 1.5 and tier1_premium > 0:
        score += 1
        reasons.append("tier1 aggressive flow")
    elif tier2_premium > tier1_premium * 1.5 and tier2_premium > 0:
        reasons.append("tier2 build present")

    atm_flow = sum(
        float(p.get("premium", 0.0) or 0.0)
        for p in option_prints
        if p.get("is_atm")
    )
    if atm_flow > 100000:
        score += 1
        reasons.append("heavy ATM activity")

    if delta_walls and spot > 0:
        nearest_wall = delta_walls[0]
        wall_bias = nearest_wall.get("bias", "")
        distance = float(nearest_wall.get("distance_from_spot", 0.0) or 0.0)

        if abs(distance) <= 1:
            if wall_bias == "CALL WALL":
                score += 2
                reasons.append("nearest wall strongly call-heavy")
            elif wall_bias == "PUT WALL":
                score -= 2
                reasons.append("nearest wall strongly put-heavy")
        elif abs(distance) <= 3:
            if wall_bias == "CALL WALL":
                score += 1
                reasons.append("nearest wall call-heavy")
            elif wall_bias == "PUT WALL":
                score -= 1
                reasons.append("nearest wall put-heavy")

    strongest_wall = None
    if delta_walls:
        strongest_wall = max(delta_walls, key=lambda x: x.get("total_premium", 0.0))

    if strongest_wall:
        strong_bias = strongest_wall.get("bias", "")
        if strong_bias == "CALL WALL":
            score += 1
            reasons.append("strongest wall call-heavy")
        elif strong_bias == "PUT WALL":
            score -= 1
            reasons.append("strongest wall put-heavy")

    if score >= 5:
        direction = "BULLISH"
        confidence = 90
    elif score >= 3:
        direction = "BULLISH"
        confidence = 80
    elif score == 2:
        direction = "BULLISH"
        confidence = 70
    elif score == 1:
        direction = "SLIGHTLY BULLISH"
        confidence = 60
    elif score == 0:
        direction = "NEUTRAL"
        confidence = 50
    elif score == -1:
        direction = "SLIGHTLY BEARISH"
        confidence = 60
    elif score == -2:
        direction = "BEARISH"
        confidence = 70
    elif score <= -5:
        direction = "BEARISH"
        confidence = 90
    else:
        direction = "BEARISH"
        confidence = 80

    reason = ", ".join(reasons) if reasons else "Not enough data yet."
    setup, trigger = _build_setup_and_trigger(direction, spot, delta_walls)

    data["signal_summary"] = {
        "direction": direction,
        "confidence": confidence,
        "reason": reason,
        "setup": setup,
        "trigger": trigger,
    }


def _build_setup_and_trigger(direction: str, spot: float, delta_walls: list):
    if not delta_walls or spot <= 0:
        return direction, "Waiting for clearer wall structure"

    above = [w for w in delta_walls if float(w.get("strike", 0) or 0) > spot]
    below = [w for w in delta_walls if float(w.get("strike", 0) or 0) < spot]

    wall_above = min(above, key=lambda x: x.get("strike", 0)) if above else None
    wall_below = max(below, key=lambda x: x.get("strike", 0)) if below else None

    if wall_above and wall_below:
        if direction in ["BULLISH", "SLIGHTLY BULLISH"]:
            return (
                "BULLISH BREAK WATCH",
                f"Break and hold above {float(wall_above['strike']):.2f}",
            )
        if direction in ["BEARISH", "SLIGHTLY BEARISH"]:
            return (
                "BEARISH BREAK WATCH",
                f"Break and hold below {float(wall_below['strike']):.2f}",
            )
        return (
            "RANGE BETWEEN WALLS",
            f"Above {float(wall_above['strike']):.2f} opens upside, below {float(wall_below['strike']):.2f} opens downside",
        )

    if wall_above:
        return (
            "TESTING OVERHEAD WALL",
            f"Watch reaction at {float(wall_above['strike']):.2f}",
        )

    if wall_below:
        return (
            "TESTING SUPPORT WALL",
            f"Watch reaction at {float(wall_below['strike']):.2f}",
        )

    return direction, "Waiting for clearer wall structure"


def _approximate_delta(contract_type: str, strike: float, spot: float, dte: int, is_atm: bool) -> float:
    if spot <= 0 or strike <= 0:
        return 0.5 if contract_type == "C" else -0.5

    moneyness = (spot - strike) / spot

    time_adjust = 0.0
    if dte <= 7:
        time_adjust = 0.1
    elif dte <= 30:
        time_adjust = 0.05

    atm_adjust = 0.1 if is_atm else 0.0

    if contract_type == "C":
        raw_delta = 0.5 + moneyness + time_adjust + atm_adjust
        return max(0.1, min(0.9, raw_delta))

    raw_delta = -0.5 + moneyness - time_adjust - atm_adjust
    return max(-0.9, min(-0.1, raw_delta))


def _build_print_tag(contract_type: str, tier: str, is_atm: bool, premium: float) -> str:
    size_tag = "LARGE" if premium >= 50000 else "SMALL"
    atm_tag = "ATM" if is_atm else "OTM/ITM"
    return f"{contract_type} | {tier.upper()} | {atm_tag} | {size_tag}"


def _extract_underlying(option_symbol: str) -> str:
    raw = option_symbol.replace("O:", "")
    i = 0
    while i < len(raw) and not raw[i].isdigit():
        i += 1
    return raw[:i]


def _extract_contract_type(option_symbol: str) -> str:
    if "C" in option_symbol[-9:]:
        return "C"
    return "P"


def _calc_dte(expiry: str) -> int:
    if not expiry:
        return 999
    try:
        exp = datetime.strptime(expiry, "%Y-%m-%d").date()
        return max((exp - datetime.now().date()).days, 0)
    except Exception:
        return 999