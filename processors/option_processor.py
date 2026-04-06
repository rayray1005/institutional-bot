from datetime import datetime
import re

from config import ATM_PCT_BAND, TIER1_MAX_DTE, TIER2_MAX_DTE
from data_store import (
    market_data,
    append_to_deque,
    get_live_snapshot,
    increment_dict_value,
    increment_field,
    replace_list,
    set_dict_value,
    set_field,
)
from analytics.summary_utils import update_summary_blocks, generate_support_resistance_zones


TOP_OPTION_TRADES_LIMIT = 25
DELTA_WALL_COUNT = 8
GAMMA_ZONE_COUNT = 5


def process_option_trade(event: dict):
    option_symbol = (event.get("sym") or event.get("symbol") or "").strip()
    parsed = _parse_option_symbol(option_symbol)

    underlying = (event.get("underlying") or parsed.get("underlying") or "").upper()
    if not underlying or underlying not in market_data:
        return

    price = _to_float(event.get("p") or event.get("price") or 0.0)
    contracts = _to_int(event.get("s") or event.get("size") or event.get("contracts") or 0)

    if price <= 0 or contracts <= 0:
        return

    snap_before = get_live_snapshot(underlying)
    spot = _to_float(snap_before.get("price", 0.0), 0.0)

    strike = _to_float(event.get("strike") or parsed.get("strike") or 0.0, 0.0)
    expiry = (
        event.get("expiry")
        or event.get("expiration")
        or parsed.get("expiry")
        or ""
    )
    expiry = _normalize_expiry(expiry)

    contract_type = (
        event.get("contract_type")
        or event.get("option_type")
        or parsed.get("contract_type")
        or ""
    )
    contract_type = str(contract_type).upper().strip()

    if contract_type not in ["C", "P"]:
        contract_type = _extract_contract_type(option_symbol)

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

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    print_data = {
        "timestamp": now,
        "timestamp_str": now_str,
        "option_symbol": option_symbol,
        "underlying": underlying,
        "contract": contract_type,
        "contract_type": contract_type,
        "option_type": contract_type,
        "strike": round(strike, 2),
        "expiry": expiry,
        "expiration": expiry,
        "dte": dte,
        "price": round(price, 4),
        "contracts": contracts,
        "size": contracts,
        "premium": premium,
        "side": str(event.get("side") or "unknown"),
        "delta_exposure": delta_exposure,
        "approx_delta": round(approx_delta, 4),
        "is_atm": is_atm,
        "tier": tier,
        "tag": _build_print_tag(contract_type, tier, is_atm, premium),
    }

    # -----------------------------
    # BASIC LIVE COUNTERS
    # -----------------------------
    increment_field(underlying, "option_events", 1)
    set_field(underlying, "last_option_update", now.strftime("%H:%M:%S"))

    # -----------------------------
    # STORE RAW PRINT
    # -----------------------------
    append_to_deque(underlying, "option_prints", print_data)

    # -----------------------------
    # SIDE TOTALS
    # -----------------------------
    if contract_type == "C":
        increment_field(underlying, "call_premium", premium)
        increment_field(underlying, "call_trades", 1)
    else:
        increment_field(underlying, "put_premium", premium)
        increment_field(underlying, "put_trades", 1)

    # -----------------------------
    # STRIKE MAPS
    # -----------------------------
    if strike > 0:
        strike_key = round(float(strike), 2)

        if contract_type == "C":
            increment_dict_value(underlying, "call_premium_by_strike", strike_key, premium)
            increment_dict_value(underlying, "call_volume_by_strike", strike_key, contracts)
        else:
            increment_dict_value(underlying, "put_premium_by_strike", strike_key, premium)
            increment_dict_value(underlying, "put_volume_by_strike", strike_key, contracts)

        increment_dict_value(underlying, "total_premium_by_strike", strike_key, premium)
        set_dict_value(underlying, "strike_last_update", strike_key, now_str)

    # -----------------------------
    # TIER STATS
    # -----------------------------
    snap_mid = get_live_snapshot(underlying)
    tier_stats = snap_mid.get("tier_stats", {}) or {
        "tier1": {"count": 0, "premium": 0.0},
        "tier2": {"count": 0, "premium": 0.0},
        "other": {"count": 0, "premium": 0.0},
    }

    if tier not in tier_stats:
        tier_stats[tier] = {"count": 0, "premium": 0.0}

    tier_stats[tier]["count"] = _to_int(tier_stats[tier].get("count", 0), 0) + 1
    tier_stats[tier]["premium"] = _to_float(tier_stats[tier].get("premium", 0.0), 0.0) + premium
    set_field(underlying, "tier_stats", tier_stats)

    # -----------------------------
    # REBUILD DERIVED PANELS
    # -----------------------------
    snap = get_live_snapshot(underlying)

    option_prints = list(snap.get("option_prints", []))
    spot = _to_float(snap.get("price", 0.0), 0.0)

    top_option_trades = _build_top_option_trades(option_prints)
    delta_walls = _build_delta_walls_from_prints_and_maps(snap, option_prints, spot)
    gamma_zones = _build_gamma_zones_from_prints_and_maps(snap, option_prints, spot)
    signal_summary = _build_signal_summary(snap, option_prints, delta_walls, spot)

    replace_list(underlying, "top_option_trades", top_option_trades)
    replace_list(underlying, "delta_walls", delta_walls)
    replace_list(underlying, "gamma_zones", gamma_zones)
    set_field(underlying, "signal_summary", signal_summary)

    generate_support_resistance_zones(underlying)
    update_summary_blocks(underlying)


# =========================================================
# BUILDERS
# =========================================================

def _build_top_option_trades(option_prints: list) -> list:
    rows = []

    for raw in option_prints:
        row = dict(raw)

        row["timestamp"] = row.get("timestamp_str") or _normalize_timestamp(row.get("timestamp"))
        row["expiration"] = _normalize_expiry(row.get("expiration") or row.get("expiry") or "")
        row["expiry"] = row["expiration"]
        row["option_type"] = str(
            row.get("option_type") or row.get("contract_type") or row.get("contract") or ""
        ).upper()
        row["strike"] = round(_to_float(row.get("strike", 0.0), 0.0), 2)
        row["premium"] = round(_to_float(row.get("premium", 0.0), 0.0), 2)
        row["size"] = _to_int(row.get("size") or row.get("contracts") or 0, 0)

        rows.append(row)

    rows.sort(
        key=lambda x: (
            _to_float(x.get("premium", 0.0), 0.0),
            _to_int(x.get("size", 0), 0),
        ),
        reverse=True,
    )

    return rows[:TOP_OPTION_TRADES_LIMIT]


def _build_delta_walls_from_prints_and_maps(snapshot: dict, option_prints: list, spot: float) -> list:
    total_map = snapshot.get("total_premium_by_strike", {}) or {}
    call_map = snapshot.get("call_premium_by_strike", {}) or {}
    put_map = snapshot.get("put_premium_by_strike", {}) or {}

    if not total_map and not option_prints:
        return []

    strike_meta = _build_strike_meta_from_prints(option_prints)

    rows = []
    all_strikes = set()

    for k in total_map.keys():
        strike = _normalize_strike(k)
        if strike is not None:
            all_strikes.add(strike)

    for k in strike_meta.keys():
        strike = _normalize_strike(k)
        if strike is not None:
            all_strikes.add(strike)

    for strike in all_strikes:
        call_premium = _map_get_float(call_map, strike)
        put_premium = _map_get_float(put_map, strike)
        total_premium = _map_get_float(total_map, strike)

        meta = strike_meta.get(strike, {})
        if total_premium <= 0:
            total_premium = _to_float(meta.get("total_premium", 0.0), 0.0)

        call_contracts = _to_int(meta.get("call_contracts", 0), 0)
        put_contracts = _to_int(meta.get("put_contracts", 0), 0)
        total_contracts = call_contracts + put_contracts

        if call_premium > put_premium * 1.15:
            bias = "CALL WALL"
        elif put_premium > call_premium * 1.15:
            bias = "PUT WALL"
        else:
            bias = "MIXED WALL"

        distance = round(strike - float(spot or 0.0), 2) if spot > 0 else 0.0
        imbalance = round(abs(call_premium - put_premium), 2)

        rows.append(
            {
                "strike": round(strike, 2),
                "expiration": meta.get("dominant_expiration", ""),
                "expirations": ", ".join(meta.get("top_expirations", [])),
                "bias": bias,
                "call_premium": round(call_premium, 2),
                "put_premium": round(put_premium, 2),
                "total_premium": round(total_premium, 2),
                "call_contracts": call_contracts,
                "put_contracts": put_contracts,
                "total_contracts": total_contracts,
                "distance_from_spot": distance,
                "imbalance": imbalance,
                "timestamp": meta.get("last_timestamp", ""),
            }
        )

    strongest_rows = sorted(
        rows,
        key=lambda x: (x["total_premium"], x["imbalance"], x["total_contracts"]),
        reverse=True,
    )[:DELTA_WALL_COUNT]

    if spot > 0:
        nearest_rows = sorted(
            rows,
            key=lambda x: (
                abs(x["distance_from_spot"]),
                -x["total_premium"],
                -x["imbalance"],
                -x["total_contracts"],
            )
        )[:DELTA_WALL_COUNT]
    else:
        nearest_rows = []

    merged = []
    seen = set()

    for row in nearest_rows + strongest_rows:
        strike = row["strike"]
        if strike not in seen:
            seen.add(strike)
            merged.append(row)

    if spot > 0:
        merged.sort(
            key=lambda x: (
                abs(x["distance_from_spot"]),
                -x["total_premium"],
                -x["imbalance"],
            )
        )
    else:
        merged.sort(
            key=lambda x: (
                -x["total_premium"],
                -x["imbalance"],
            )
        )

    return merged[:DELTA_WALL_COUNT]


def _build_gamma_zones_from_prints_and_maps(snapshot: dict, option_prints: list, spot: float) -> list:
    total_map = snapshot.get("total_premium_by_strike", {}) or {}
    call_vol_map = snapshot.get("call_volume_by_strike", {}) or {}
    put_vol_map = snapshot.get("put_volume_by_strike", {}) or {}

    if not total_map and not option_prints:
        return []

    strike_meta = _build_strike_meta_from_prints(option_prints)

    rows = []
    all_strikes = set()

    for k in total_map.keys():
        strike = _normalize_strike(k)
        if strike is not None:
            all_strikes.add(strike)

    for k in strike_meta.keys():
        strike = _normalize_strike(k)
        if strike is not None:
            all_strikes.add(strike)

    for strike in all_strikes:
        total_premium = _map_get_float(total_map, strike)
        meta = strike_meta.get(strike, {})

        if total_premium <= 0:
            total_premium = _to_float(meta.get("total_premium", 0.0), 0.0)

        call_volume = _map_get_int(call_vol_map, strike)
        put_volume = _map_get_int(put_vol_map, strike)

        if call_volume <= 0:
            call_volume = _to_int(meta.get("call_contracts", 0), 0)
        if put_volume <= 0:
            put_volume = _to_int(meta.get("put_contracts", 0), 0)

        total_volume = call_volume + put_volume
        distance = round(strike - float(spot or 0.0), 2) if spot > 0 else 0.0

        distance_boost = (1 / (abs(distance) + 1)) * 10000 if spot > 0 else 0.0
        zone_strength = round(
            (total_premium * 0.70) +
            (total_volume * 20) +
            distance_boost,
            2,
        )

        if call_volume > put_volume * 1.15:
            bias = "CALL HEAVY"
        elif put_volume > call_volume * 1.15:
            bias = "PUT HEAVY"
        else:
            bias = "MIXED"

        rows.append(
            {
                "strike": round(strike, 2),
                "expiration": meta.get("dominant_expiration", ""),
                "expirations": ", ".join(meta.get("top_expirations", [])),
                "bias": bias,
                "total_premium": round(total_premium, 2),
                "call_contracts": call_volume,
                "put_contracts": put_volume,
                "total_contracts": total_volume,
                "distance_from_spot": distance,
                "zone_strength": zone_strength,
                "timestamp": meta.get("last_timestamp", ""),
            }
        )

    strongest_rows = sorted(
        rows,
        key=lambda x: x["zone_strength"],
        reverse=True,
    )[:GAMMA_ZONE_COUNT]

    if spot > 0:
        nearest_rows = sorted(
            rows,
            key=lambda x: (
                abs(x["distance_from_spot"]),
                -x["zone_strength"],
                -x["total_premium"],
            )
        )[:GAMMA_ZONE_COUNT]
    else:
        nearest_rows = []

    merged = []
    seen = set()

    for row in nearest_rows + strongest_rows:
        strike = row["strike"]
        if strike not in seen:
            seen.add(strike)
            merged.append(row)

    if spot > 0:
        merged.sort(
            key=lambda x: (
                abs(x["distance_from_spot"]),
                -x["zone_strength"],
                -x["total_premium"],
            )
        )
    else:
        merged.sort(key=lambda x: x["zone_strength"], reverse=True)

    return merged[:GAMMA_ZONE_COUNT]


def _build_signal_summary(snapshot: dict, option_prints: list, delta_walls: list, spot: float) -> dict:
    call_premium = _to_float(snapshot.get("call_premium", 0.0), 0.0)
    put_premium = _to_float(snapshot.get("put_premium", 0.0), 0.0)
    net_flow = _to_float(snapshot.get("net_flow", 0.0), 0.0)
    tier_stats = snapshot.get("tier_stats", {}) or {}

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

    tier1_premium = _to_float(tier_stats.get("tier1", {}).get("premium", 0.0), 0.0)
    tier2_premium = _to_float(tier_stats.get("tier2", {}).get("premium", 0.0), 0.0)

    if tier1_premium > tier2_premium * 1.5 and tier1_premium > 0:
        score += 1
        reasons.append("tier1 aggressive flow")
    elif tier2_premium > tier1_premium * 1.5 and tier2_premium > 0:
        reasons.append("tier2 build present")

    atm_flow = sum(
        _to_float(p.get("premium", 0.0), 0.0)
        for p in option_prints
        if p.get("is_atm")
    )
    if atm_flow > 100000:
        score += 1
        reasons.append("heavy ATM activity")

    if delta_walls and spot > 0:
        nearest_wall = min(
            delta_walls,
            key=lambda x: abs(_to_float(x.get("distance_from_spot", 9999), 9999))
        )
        wall_bias = nearest_wall.get("bias", "")
        distance = _to_float(nearest_wall.get("distance_from_spot", 0.0), 0.0)

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

    strongest_wall = max(delta_walls, key=lambda x: x.get("total_premium", 0.0)) if delta_walls else None
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

    return {
        "direction": direction,
        "confidence": confidence,
        "reason": reason,
        "setup": setup,
        "trigger": trigger,
    }


def _build_strike_meta_from_prints(option_prints: list) -> dict:
    bucket = {}

    for raw in option_prints:
        strike = _normalize_strike(raw.get("strike"))
        if strike is None or strike <= 0:
            continue

        premium = _to_float(raw.get("premium", 0.0), 0.0)
        contracts = _to_int(raw.get("contracts") or raw.get("size") or 0, 0)
        expiry = _normalize_expiry(raw.get("expiration") or raw.get("expiry") or "")
        option_type = str(
            raw.get("option_type") or raw.get("contract_type") or raw.get("contract") or ""
        ).upper().strip()
        timestamp = raw.get("timestamp_str") or _normalize_timestamp(raw.get("timestamp"))

        if strike not in bucket:
            bucket[strike] = {
                "total_premium": 0.0,
                "call_contracts": 0,
                "put_contracts": 0,
                "expiry_premium": {},
                "last_timestamp": "",
            }

        bucket[strike]["total_premium"] += premium

        if option_type == "C":
            bucket[strike]["call_contracts"] += contracts
        elif option_type == "P":
            bucket[strike]["put_contracts"] += contracts

        if expiry:
            bucket[strike]["expiry_premium"][expiry] = (
                bucket[strike]["expiry_premium"].get(expiry, 0.0) + premium
            )

        if timestamp:
            bucket[strike]["last_timestamp"] = timestamp

    out = {}
    for strike, meta in bucket.items():
        expiry_premium = meta.get("expiry_premium", {})
        ranked_exp = sorted(
            expiry_premium.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        dominant_expiration = ranked_exp[0][0] if ranked_exp else ""
        top_expirations = [exp for exp, _ in ranked_exp[:3]]

        out[strike] = {
            "total_premium": round(meta.get("total_premium", 0.0), 2),
            "call_contracts": meta.get("call_contracts", 0),
            "put_contracts": meta.get("put_contracts", 0),
            "dominant_expiration": dominant_expiration,
            "top_expirations": top_expirations,
            "last_timestamp": meta.get("last_timestamp", ""),
        }

    return out


# =========================================================
# HELPERS
# =========================================================

def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def _normalize_strike(value):
    try:
        return round(float(value), 2)
    except Exception:
        return None


def _map_get_float(d: dict, strike: float) -> float:
    if not isinstance(d, dict):
        return 0.0

    candidates = [strike, round(float(strike), 2), str(strike), str(round(float(strike), 2))]
    for key in candidates:
        if key in d:
            return _to_float(d.get(key), 0.0)

    for k, v in d.items():
        try:
            if round(float(k), 2) == round(float(strike), 2):
                return _to_float(v, 0.0)
        except Exception:
            continue

    return 0.0


def _map_get_int(d: dict, strike: float) -> int:
    if not isinstance(d, dict):
        return 0

    candidates = [strike, round(float(strike), 2), str(strike), str(round(float(strike), 2))]
    for key in candidates:
        if key in d:
            return _to_int(d.get(key), 0)

    for k, v in d.items():
        try:
            if round(float(k), 2) == round(float(strike), 2):
                return _to_int(v, 0)
        except Exception:
            continue

    return 0


def _normalize_timestamp(value):
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    try:
        return str(value)
    except Exception:
        return ""


def _normalize_expiry(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip()
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except Exception:
        pass

    try:
        return datetime.strptime(value, "%y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return value


def _parse_option_symbol(option_symbol: str) -> dict:
    raw = (option_symbol or "").replace("O:", "").strip().upper()

    if not raw:
        return {"underlying": "", "expiry": "", "contract_type": "", "strike": 0.0}

    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", raw)
    if not match:
        return {
            "underlying": _extract_underlying_fallback(raw),
            "expiry": "",
            "contract_type": _extract_contract_type(raw),
            "strike": 0.0,
        }

    underlying, exp_part, contract_type, strike_part = match.groups()

    expiry = ""
    try:
        yy = int(exp_part[:2])
        mm = int(exp_part[2:4])
        dd = int(exp_part[4:6])
        expiry = f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        expiry = ""

    strike = 0.0
    try:
        strike = int(strike_part) / 1000.0
    except Exception:
        strike = 0.0

    return {
        "underlying": underlying,
        "expiry": expiry,
        "contract_type": contract_type,
        "strike": strike,
    }


def _extract_underlying_fallback(raw: str) -> str:
    idx = 0
    while idx < len(raw) and not raw[idx].isdigit():
        idx += 1
    return raw[:idx]


def _extract_contract_type(option_symbol: str) -> str:
    raw = (option_symbol or "").replace("O:", "").upper()

    for ch in ["C", "P"]:
        pos = raw.rfind(ch)
        if pos != -1 and pos >= len(raw) - 9:
            return ch

    return ""


def _calc_dte(expiry: str) -> int:
    if not expiry:
        return 999
    try:
        exp = datetime.strptime(expiry, "%Y-%m-%d").date()
        return max((exp - datetime.now().date()).days, 0)
    except Exception:
        return 999


def _approximate_delta(contract_type: str, strike: float, spot: float, dte: int, is_atm: bool) -> float:
    if spot <= 0 or strike <= 0:
        return 0.5 if contract_type == "C" else -0.5

    moneyness = (spot - strike) / spot

    time_adjust = 0.0
    if dte <= 7:
        time_adjust = 0.10
    elif dte <= 30:
        time_adjust = 0.05

    atm_adjust = 0.10 if is_atm else 0.0

    if contract_type == "C":
        raw_delta = 0.5 + moneyness + time_adjust + atm_adjust
        return max(0.1, min(0.9, raw_delta))

    raw_delta = -0.5 + moneyness - time_adjust - atm_adjust
    return max(-0.9, min(-0.1, raw_delta))


def _build_print_tag(contract_type: str, tier: str, is_atm: bool, premium: float) -> str:
    size_tag = "LARGE" if premium >= 50000 else "SMALL"
    atm_tag = "ATM" if is_atm else "OTM/ITM"
    return f"{contract_type} | {tier.upper()} | {atm_tag} | {size_tag}"


def _build_setup_and_trigger(direction: str, spot: float, delta_walls: list):
    if not delta_walls or spot <= 0:
        return direction, "Waiting for clearer wall structure"

    above = [w for w in delta_walls if _to_float(w.get("strike", 0), 0.0) > spot]
    below = [w for w in delta_walls if _to_float(w.get("strike", 0), 0.0) < spot]

    wall_above = min(above, key=lambda x: x.get("strike", 0)) if above else None
    wall_below = max(below, key=lambda x: x.get("strike", 0)) if below else None

    if wall_above and wall_below:
        if direction in ["BULLISH", "SLIGHTLY BULLISH"]:
            return (
                "BULLISH BREAK WATCH",
                f"Break and hold above {_to_float(wall_above['strike'], 0.0):.2f}",
            )
        if direction in ["BEARISH", "SLIGHTLY BEARISH"]:
            return (
                "BEARISH BREAK WATCH",
                f"Break and hold below {_to_float(wall_below['strike'], 0.0):.2f}",
            )
        return (
            "RANGE BETWEEN WALLS",
            f"Above {_to_float(wall_above['strike'], 0.0):.2f} opens upside, below {_to_float(wall_below['strike'], 0.0):.2f} opens downside",
        )

    if wall_above:
        return (
            "TESTING OVERHEAD WALL",
            f"Watch reaction at {_to_float(wall_above['strike'], 0.0):.2f}",
        )

    if wall_below:
        return (
            "TESTING SUPPORT WALL",
            f"Watch reaction at {_to_float(wall_below['strike'], 0.0):.2f}",
        )

    return direction, "Waiting for clearer wall structure"