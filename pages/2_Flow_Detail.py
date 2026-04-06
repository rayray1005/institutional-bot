from datetime import datetime, date
import re
import pandas as pd
import streamlit as st

from config import WATCHLIST
import data_store
from data_store import get_display_data

st.set_page_config(layout="wide", page_title="Flow Detail")

st.title("Flow Detail")


# -----------------------------
# HELPERS
# -----------------------------
def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def fmt_money(value):
    try:
        return f"${float(value):,.0f}"
    except (TypeError, ValueError):
        return "$0"


def normalize_strike(value):
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def normalize_strike_dict(d):
    normalized = {}

    if not isinstance(d, dict):
        return normalized

    for k, v in d.items():
        strike = normalize_strike(k)
        if strike is None:
            continue

        value = to_float(v, 0.0)
        normalized[strike] = normalized.get(strike, 0.0) + value

    return normalized


def normalize_any_timestamp(value):
    if value in [None, ""]:
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def normalize_expiration(value):
    if value in [None, ""]:
        return ""

    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def sanitize_dataframe_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DataFrame safe for Streamlit / PyArrow rendering.
    Converts datetime/object/mixed columns into clean strings where needed.
    """
    if df is None:
        return df

    if df.empty:
        return df.copy()

    df = df.copy()

    for col in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                continue
        except Exception:
            pass

        if df[col].dtype == "object":
            def _safe_value(x):
                if x is None:
                    return ""
                try:
                    if pd.isna(x):
                        return ""
                except Exception:
                    pass

                if isinstance(x, (datetime, pd.Timestamp)):
                    return x.strftime("%Y-%m-%d %H:%M:%S")

                if isinstance(x, date):
                    return x.strftime("%Y-%m-%d")

                if isinstance(x, (dict, list, tuple, set)):
                    return str(x)

                return str(x)

            df[col] = df[col].apply(_safe_value)

    return df


def first_present(row, keys, default=""):
    for key in keys:
        if key in row and row[key] not in [None, ""]:
            return row[key]
    return default


def parse_option_symbol_fallback(option_symbol):
    raw = str(option_symbol or "").replace("O:", "").strip().upper()

    result = {
        "expiration": "",
        "option_type": "",
        "strike": "",
    }

    if not raw:
        return result

    match = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", raw)
    if not match:
        return result

    _, exp_part, option_type, strike_part = match.groups()

    try:
        yy = int(exp_part[:2])
        mm = int(exp_part[2:4])
        dd = int(exp_part[4:6])
        result["expiration"] = f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        result["expiration"] = ""

    try:
        result["strike"] = round(int(strike_part) / 1000.0, 2)
    except Exception:
        result["strike"] = ""

    result["option_type"] = option_type
    return result


def coerce_trade_row(row):
    row = dict(row)

    strike = first_present(
        row,
        ["strike", "Strike", "option_strike", "contract_strike"],
        default="",
    )
    expiration = first_present(
        row,
        ["expiration", "expiry", "exp", "expiration_date", "expiry_date"],
        default="",
    )
    option_type = first_present(
        row,
        ["option_type", "type", "cp", "contract_type", "contract"],
        default="",
    )
    premium = first_present(
        row,
        ["premium", "notional", "total_premium", "value"],
        default=0.0,
    )
    size = first_present(
        row,
        ["size", "contracts", "qty", "volume"],
        default=0,
    )
    timestamp = first_present(
        row,
        ["timestamp", "time", "ts", "timestamp_str"],
        default="",
    )
    option_symbol = first_present(
        row,
        ["option_symbol", "sym", "symbol"],
        default="",
    )

    parsed = parse_option_symbol_fallback(option_symbol)

    if strike in ["", None, 0, 0.0]:
        strike = parsed["strike"]

    if expiration in ["", None, "UNKNOWN"]:
        expiration = parsed["expiration"]

    if option_type in ["", None, "UNKNOWN", "unknown"]:
        option_type = parsed["option_type"]

    row["strike"] = to_float(strike, 0.0) if strike not in ["", None] else 0.0
    row["expiration"] = normalize_expiration(expiration)
    row["option_type"] = str(option_type).upper() if option_type not in [None, ""] else ""
    row["premium"] = to_float(premium, 0.0)
    row["size"] = to_int(size, 0)
    row["timestamp"] = normalize_any_timestamp(timestamp)
    row["timestamp_str"] = row["timestamp"]
    row["option_symbol"] = option_symbol

    return row


def enrich_wall_row(row, spot_price):
    wall = dict(row)

    strike = to_float(first_present(wall, ["strike", "Strike"], 0.0), 0.0)
    call_prem = to_float(first_present(wall, ["call_premium", "Call Premium"], 0.0), 0.0)
    put_prem = to_float(first_present(wall, ["put_premium", "Put Premium"], 0.0), 0.0)
    total_prem = to_float(
        first_present(wall, ["total_premium", "Total Premium", "premium"], 0.0),
        0.0,
    )
    bias = first_present(wall, ["bias", "Bias"], "")
    expiration = normalize_expiration(
        first_present(wall, ["expiration", "expiry", "exp", "expiration_date"], "")
    )
    expirations = first_present(wall, ["expirations", "top_expirations"], "")
    timestamp = normalize_any_timestamp(
        first_present(wall, ["timestamp", "time", "ts", "timestamp_str"], "")
    )

    call_contracts = to_int(first_present(wall, ["call_contracts", "call_volume", "Call Vol"], 0), 0)
    put_contracts = to_int(first_present(wall, ["put_contracts", "put_volume", "Put Vol"], 0), 0)
    total_contracts = to_int(first_present(wall, ["total_contracts", "total_volume"], 0), 0)
    zone_strength = to_float(first_present(wall, ["zone_strength"], 0.0), 0.0)
    imbalance = to_float(first_present(wall, ["imbalance"], 0.0), 0.0)

    if not total_prem:
        total_prem = call_prem + put_prem

    if not total_contracts:
        total_contracts = call_contracts + put_contracts

    distance = strike - spot_price if spot_price > 0 else 0.0

    wall["strike"] = round(strike, 2)
    wall["call_premium"] = round(call_prem, 2)
    wall["put_premium"] = round(put_prem, 2)
    wall["total_premium"] = round(total_prem, 2)
    wall["bias"] = bias if bias else (
        "CALL HEAVY" if call_prem > put_prem
        else "PUT HEAVY" if put_prem > call_prem
        else "MIXED"
    )
    wall["expiration"] = expiration
    wall["expirations"] = expirations
    wall["call_contracts"] = call_contracts
    wall["put_contracts"] = put_contracts
    wall["total_contracts"] = total_contracts
    wall["zone_strength"] = round(zone_strength, 2)
    wall["imbalance"] = round(imbalance, 2)
    wall["distance_from_spot"] = round(distance, 2)
    wall["timestamp"] = timestamp
    wall["timestamp_str"] = timestamp

    return wall


def build_trade_setup(spot_price, wall_above, wall_below, direction, confidence):
    if spot_price <= 0:
        return {
            "setup": "NO DATA",
            "trigger": "Waiting for live price.",
            "risk": "No setup yet.",
        }

    if wall_above and wall_below:
        dist_above = abs(wall_above["strike"] - spot_price)
        dist_below = abs(spot_price - wall_below["strike"])

        if dist_above <= 1.0 and dist_below <= 1.0:
            return {
                "setup": "PIN / SQUEEZE ZONE",
                "trigger": (
                    f"Price trapped between {wall_below['strike']:.2f} and "
                    f"{wall_above['strike']:.2f}. Watch for break."
                ),
                "risk": "Expect chop until one side clearly breaks.",
            }

        if direction in ["BULLISH", "SLIGHTLY BULLISH"] and dist_above <= 3:
            return {
                "setup": "BULLISH BREAK WATCH",
                "trigger": f"Break and hold above {wall_above['strike']:.2f}.",
                "risk": f"Failure back below {wall_above['strike']:.2f} can trap buyers.",
            }

        if direction in ["BEARISH", "SLIGHTLY BEARISH"] and dist_below <= 3:
            return {
                "setup": "BEARISH BREAK WATCH",
                "trigger": f"Break and hold below {wall_below['strike']:.2f}.",
                "risk": f"Failure back above {wall_below['strike']:.2f} can trap sellers.",
            }

        return {
            "setup": "RANGE BETWEEN WALLS",
            "trigger": (
                f"Above {wall_above['strike']:.2f} may open upside. "
                f"Below {wall_below['strike']:.2f} may open downside."
            ),
            "risk": "Middle of range can stay noisy.",
        }

    if wall_above:
        dist_above = abs(wall_above["strike"] - spot_price)
        if dist_above <= 3:
            return {
                "setup": "TESTING OVERHEAD WALL",
                "trigger": f"Watch reaction at {wall_above['strike']:.2f}.",
                "risk": "Rejection can send price back down.",
            }

    if wall_below:
        dist_below = abs(spot_price - wall_below["strike"])
        if dist_below <= 3:
            return {
                "setup": "TESTING SUPPORT WALL",
                "trigger": f"Watch reaction at {wall_below['strike']:.2f}.",
                "risk": f"Loss of {wall_below['strike']:.2f} can accelerate down.",
            }

    if confidence >= 70:
        return {
            "setup": f"{direction} FLOW BIAS",
            "trigger": "Follow strongest prints, nearest wall reaction, and range edges.",
            "risk": "Needs confirmation from live price movement.",
        }

    return {
        "setup": "NO CLEAN SETUP",
        "trigger": "Wait for stronger wall alignment or fresh prints.",
        "risk": "Low edge right now.",
    }


def build_tier_trade_table(option_prints_list, min_premium, max_premium=None, label=""):
    qualified_rows = []
    fallback_rows = []

    for raw in option_prints_list:
        row = coerce_trade_row(raw)
        premium = row["premium"]

        display_row = {
            "Tier": label,
            "Timestamp": row["timestamp"],
            "Expiration": row["expiration"] if row["expiration"] else "UNKNOWN",
            "Type": row["option_type"] if row["option_type"] else "UNKNOWN",
            "Strike": row["strike"] if row["strike"] not in ["", None] else 0.0,
            "Premium": round(premium, 2),
            "Size": row["size"],
            "Option Symbol": row.get("option_symbol", raw.get("option_symbol", "")),
        }

        fallback_rows.append(display_row)

        if premium < min_premium:
            continue
        if max_premium is not None and premium >= max_premium:
            continue

        qualified_rows.append(display_row)

    qualified_rows = sorted(
        qualified_rows,
        key=lambda x: x["Premium"],
        reverse=True,
    )

    if qualified_rows:
        return qualified_rows[:25]

    fallback_rows = sorted(
        fallback_rows,
        key=lambda x: x["Premium"],
        reverse=True,
    )

    return fallback_rows[:10]


def build_direction_scorecard(spot_price, support_price, resistance_price, direction, confidence, nearest, strongest):
    notes = []

    if direction in ["BULLISH", "SLIGHTLY BULLISH"]:
        notes.append("Flow bias favors upside continuation.")
    elif direction in ["BEARISH", "SLIGHTLY BEARISH"]:
        notes.append("Flow bias favors downside continuation.")
    else:
        notes.append("Flow is neutral; wait for cleaner confirmation.")

    if nearest:
        notes.append(
            f"Nearest wall: {nearest.get('strike', 0):.2f} "
            f"({nearest.get('bias', 'MIXED')}, {nearest.get('distance_from_spot', 0):+})."
        )

    if strongest:
        notes.append(
            f"Strongest wall: {strongest.get('strike', 0):.2f} "
            f"with {fmt_money(strongest.get('total_premium', 0))}."
        )

    if support_price > 0 and resistance_price > 0:
        notes.append(
            f"Live range: support {support_price:.2f} / resistance {resistance_price:.2f}."
        )

        if spot_price <= support_price + 0.25:
            notes.append("Price is trading near support.")
        elif spot_price >= resistance_price - 0.25:
            notes.append("Price is trading near resistance.")
        else:
            notes.append("Price is trading inside the middle of the range.")

    return {
        "direction": direction,
        "confidence": confidence,
        "notes": " ".join(notes),
    }


# -----------------------------
# DATA
# -----------------------------
selected = st.selectbox("Select ticker", WATCHLIST, key="flow_ticker")
data = get_display_data(selected)

spot = to_float(data.get("price", 0.0), 0.0)

buy_flow = to_float(data.get("buy_flow", 0.0), 0.0)
sell_flow = to_float(data.get("sell_flow", 0.0), 0.0)
net_flow = to_float(data.get("net_flow", 0.0), 0.0)

call_premium = to_float(data.get("call_premium", 0.0), 0.0)
put_premium = to_float(data.get("put_premium", 0.0), 0.0)
options_bias = data.get("options_bias", "NEUTRAL")

trade_count = to_int(data.get("trade_count", 0), 0)
avg_trade_size = to_float(data.get("avg_trade_size", 0.0), 0.0)
classification_rate = to_float(data.get("classification_rate", 0.0), 0.0)

support = to_float(data.get("support", 0.0), 0.0)
resistance = to_float(data.get("resistance", 0.0), 0.0)

big_trades = list(data.get("big_trades", []))
option_prints = list(data.get("option_prints", []))
top_option_trades = list(data.get("top_option_trades", []))
delta_walls = list(data.get("delta_walls", []))
gamma_zones = list(data.get("gamma_zones", []))
tier_stats = data.get("tier_stats", {})
signal_summary = data.get("signal_summary", {})

call_premium_by_strike = normalize_strike_dict(data.get("call_premium_by_strike", {}))
put_premium_by_strike = normalize_strike_dict(data.get("put_premium_by_strike", {}))
total_premium_by_strike = normalize_strike_dict(data.get("total_premium_by_strike", {}))
call_volume_by_strike = normalize_strike_dict(data.get("call_volume_by_strike", {}))
put_volume_by_strike = normalize_strike_dict(data.get("put_volume_by_strike", {}))

raw_strike_last_update = data.get("strike_last_update", {})
strike_last_update = {}
if isinstance(raw_strike_last_update, dict):
    for k, v in raw_strike_last_update.items():
        strike = normalize_strike(k)
        if strike is not None:
            strike_last_update[strike] = normalize_any_timestamp(v)

signal_direction = signal_summary.get("direction", "NEUTRAL")
signal_confidence = to_int(signal_summary.get("confidence", 0), 0)
signal_reason = signal_summary.get("reason", "Not enough data yet.")

delta_walls = [enrich_wall_row(w, spot) for w in delta_walls]
gamma_zones = [enrich_wall_row(w, spot) for w in gamma_zones]

delta_walls = sorted(delta_walls, key=lambda x: x.get("total_premium", 0.0), reverse=True)
gamma_zones = sorted(gamma_zones, key=lambda x: x.get("zone_strength", x.get("total_premium", 0.0)), reverse=True)

# -----------------------------
# WALL LOGIC
# -----------------------------
above_wall = None
below_wall = None
strongest_wall = None
nearest_wall = None

if delta_walls:
    strongest_wall = max(delta_walls, key=lambda x: x.get("total_premium", 0.0))
    nearest_wall = min(delta_walls, key=lambda x: abs(x.get("distance_from_spot", 999999)))

    above = [w for w in delta_walls if w.get("strike", 0.0) > spot]
    below = [w for w in delta_walls if w.get("strike", 0.0) < spot]

    if above:
        above_wall = min(above, key=lambda x: x.get("strike", 0.0))
    if below:
        below_wall = max(below, key=lambda x: x.get("strike", 0.0))

trade_setup = build_trade_setup(
    spot,
    above_wall,
    below_wall,
    signal_direction,
    signal_confidence,
)

direction_scorecard = build_direction_scorecard(
    spot,
    support,
    resistance,
    signal_direction,
    signal_confidence,
    nearest_wall,
    strongest_wall,
)

# -----------------------------
# STRIKE MAP TABLE
# -----------------------------
strike_rows = []

all_strikes = sorted(set(total_premium_by_strike.keys()))
for strike in all_strikes:
    cp = to_float(call_premium_by_strike.get(strike, 0.0), 0.0)
    pp = to_float(put_premium_by_strike.get(strike, 0.0), 0.0)
    tv = to_float(total_premium_by_strike.get(strike, 0.0), 0.0)
    cv = to_int(call_volume_by_strike.get(strike, 0), 0)
    pv = to_int(put_volume_by_strike.get(strike, 0), 0)
    last_upd = strike_last_update.get(strike, "")

    if cp > pp * 1.15:
        strike_bias = "CALL HEAVY"
    elif pp > cp * 1.15:
        strike_bias = "PUT HEAVY"
    else:
        strike_bias = "MIXED"

    strike_rows.append(
        {
            "Strike": strike,
            "Distance": round(strike - spot, 2) if spot > 0 else 0.0,
            "Call Premium": round(cp, 2),
            "Put Premium": round(pp, 2),
            "Total Premium": round(tv, 2),
            "Call Vol": cv,
            "Put Vol": pv,
            "Bias": strike_bias,
            "Last Update": last_upd,
        }
    )

strike_rows_sorted = sorted(
    strike_rows,
    key=lambda x: (abs(x["Distance"]), -x["Total Premium"])
)[:20]

# -----------------------------
# TIER DETAILS
# -----------------------------
tier1_details = build_tier_trade_table(
    option_prints,
    min_premium=100000.0,
    max_premium=None,
    label="TIER 1",
)

tier2_details = build_tier_trade_table(
    option_prints,
    min_premium=25000.0,
    max_premium=100000.0,
    label="TIER 2",
)

# -----------------------------
# TOP OPTION PRINTS FORMAT
# -----------------------------
formatted_top_option_trades = []
for raw in top_option_trades:
    row = coerce_trade_row(raw)
    raw_copy = dict(raw)
    raw_copy["timestamp"] = row["timestamp"]
    raw_copy["timestamp_str"] = row["timestamp"]
    raw_copy["expiration"] = row["expiration"]
    raw_copy["option_type"] = row["option_type"]
    raw_copy["strike"] = row["strike"]
    raw_copy["premium"] = row["premium"]
    raw_copy["size"] = row["size"]
    formatted_top_option_trades.append(raw_copy)

formatted_top_option_trades = sorted(
    formatted_top_option_trades,
    key=lambda x: to_float(x.get("premium", x.get("notional", 0.0)), 0.0),
    reverse=True,
)

# -----------------------------
# PAGE
# -----------------------------
st.subheader("Live Signal Engine")

sig1, sig2, sig3 = st.columns(3)

with sig1:
    st.metric("Direction", signal_direction)

with sig2:
    st.metric("Confidence", f"{signal_confidence}%")

with sig3:
    st.metric("Options Bias", options_bias)

st.write(f"**Reason:** {signal_reason}")
st.info(direction_scorecard["notes"])

st.divider()

st.subheader("Key Levels (Live Walls)")

k1, k2, k3, k4 = st.columns(4)

with k1:
    if above_wall:
        st.metric(
            "Wall Above",
            f"{above_wall['strike']:.2f}",
            f"{above_wall['bias']} | {above_wall['distance_from_spot']:+}",
        )
        if above_wall.get("expiration"):
            st.caption(f"Exp: {above_wall['expiration']}")
    else:
        st.metric("Wall Above", "N/A")

with k2:
    if below_wall:
        st.metric(
            "Wall Below",
            f"{below_wall['strike']:.2f}",
            f"{below_wall['bias']} | {below_wall['distance_from_spot']:+}",
        )
        if below_wall.get("expiration"):
            st.caption(f"Exp: {below_wall['expiration']}")
    else:
        st.metric("Wall Below", "N/A")

with k3:
    if strongest_wall:
        st.metric(
            "Strongest Wall",
            f"{strongest_wall['strike']:.2f}",
            fmt_money(strongest_wall['total_premium']),
        )
        if strongest_wall.get("expiration"):
            st.caption(f"Exp: {strongest_wall['expiration']}")
    else:
        st.metric("Strongest Wall", "N/A")

with k4:
    if nearest_wall:
        st.metric(
            "Nearest Wall",
            f"{nearest_wall['strike']:.2f}",
            f"{nearest_wall['distance_from_spot']:+}",
        )
        if nearest_wall.get("expiration"):
            st.caption(f"Exp: {nearest_wall['expiration']}")
    else:
        st.metric("Nearest Wall", "N/A")

st.divider()

st.subheader("Trade Setup / Alert Box")

a1, a2, a3 = st.columns(3)

with a1:
    st.metric("Setup", trade_setup["setup"])

with a2:
    st.write("**Trigger**")
    st.write(trade_setup["trigger"])

with a3:
    st.write("**Risk**")
    st.write(trade_setup["risk"])

st.divider()

st.subheader("Spot Context")

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("Spot", f"{spot:,.2f}")

with c2:
    st.metric("Support", f"{support:,.2f}")

with c3:
    st.metric("Resistance", f"{resistance:,.2f}")

with c4:
    if support > 0 and resistance > 0:
        st.metric("Range Width", f"{(resistance - support):,.2f}")
    else:
        st.metric("Range Width", "N/A")

st.divider()

st.subheader("Flow Totals")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Buy Flow", f"{buy_flow:,.2f}")

with col2:
    st.metric("Sell Flow", f"{sell_flow:,.2f}")

with col3:
    st.metric("Net Flow", f"{net_flow:,.2f}")

with col4:
    st.metric("Trades", f"{trade_count:,}")

col5, col6, col7, col8 = st.columns(4)

with col5:
    st.metric("Call Premium", f"{call_premium:,.2f}")

with col6:
    st.metric("Put Premium", f"{put_premium:,.2f}")

with col7:
    st.metric("Big Trades", f"{len(big_trades):,}")

with col8:
    st.metric("Option Prints", f"{len(option_prints):,}")

col9, col10 = st.columns(2)

with col9:
    st.metric("Avg Trade Size", f"{avg_trade_size:,.2f}")

with col10:
    st.metric("Classification Rate", f"{classification_rate:,.2f}%")

st.divider()

st.subheader("Tier Breakdown")

tier_rows = []
for tier_name in ["tier1", "tier2", "other"]:
    stats = tier_stats.get(tier_name, {"count": 0, "premium": 0.0})
    tier_rows.append(
        {
            "Tier": tier_name.upper(),
            "Trade Count": stats.get("count", 0),
            "Premium": round(to_float(stats.get("premium", 0.0), 0.0), 2),
        }
    )

tier_df = pd.DataFrame(tier_rows)
tier_df = sanitize_dataframe_for_streamlit(tier_df)
st.dataframe(tier_df, width="stretch", height=180)

st.divider()

tleft, tright = st.columns(2)

with tleft:
    st.subheader("Tier 1 Prints")
    if tier1_details:
        tier1_df = pd.DataFrame(tier1_details)
        tier1_df = sanitize_dataframe_for_streamlit(tier1_df)
        st.dataframe(tier1_df, width="stretch", height=300)
    else:
        st.info("No Tier 1 prints in the current rolling window.")

with tright:
    st.subheader("Tier 2 Prints")
    if tier2_details:
        tier2_df = pd.DataFrame(tier2_details)
        tier2_df = sanitize_dataframe_for_streamlit(tier2_df)
        st.dataframe(tier2_df, width="stretch", height=300)
    else:
        st.info("No Tier 2 prints in the current rolling window.")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Delta Walls (Largest First)")

    if delta_walls:
        delta_walls_df = pd.DataFrame(delta_walls)
        preferred_cols = [
            "strike",
            "expiration",
            "expirations",
            "bias",
            "call_premium",
            "put_premium",
            "total_premium",
            "call_contracts",
            "put_contracts",
            "total_contracts",
            "imbalance",
            "distance_from_spot",
            "timestamp",
        ]
        cols = [c for c in preferred_cols if c in delta_walls_df.columns]
        if cols:
            delta_walls_df = delta_walls_df[cols]
        delta_walls_df = sanitize_dataframe_for_streamlit(delta_walls_df)
        st.dataframe(delta_walls_df, width="stretch", height=340)
    else:
        st.info("No delta walls yet.")

with right:
    st.subheader("Gamma Zones (Largest First)")

    if gamma_zones:
        gamma_zones_df = pd.DataFrame(gamma_zones)
        preferred_cols = [
            "strike",
            "expiration",
            "expirations",
            "bias",
            "total_premium",
            "call_contracts",
            "put_contracts",
            "total_contracts",
            "zone_strength",
            "distance_from_spot",
            "timestamp",
        ]
        cols = [c for c in preferred_cols if c in gamma_zones_df.columns]
        if cols:
            gamma_zones_df = gamma_zones_df[cols]
        gamma_zones_df = sanitize_dataframe_for_streamlit(gamma_zones_df)
        st.dataframe(gamma_zones_df, width="stretch", height=340)
    else:
        st.info("No gamma zones yet.")

st.divider()

st.subheader("Strike Map Near Spot")

if strike_rows_sorted:
    strike_df = pd.DataFrame(strike_rows_sorted)
    strike_df = sanitize_dataframe_for_streamlit(strike_df)
    st.dataframe(strike_df, width="stretch", height=360)
else:
    st.info("No strike map data yet.")

st.divider()

st.subheader("Top Option Prints")

if formatted_top_option_trades:
    top_option_trades_df = pd.DataFrame(formatted_top_option_trades)

    preferred_cols = [
        "timestamp",
        "expiration",
        "option_type",
        "strike",
        "premium",
        "size",
        "price",
        "side",
        "option_symbol",
        "underlying",
        "dte",
        "tier",
        "tag",
    ]
    existing_cols = [c for c in preferred_cols if c in top_option_trades_df.columns]
    other_cols = [c for c in top_option_trades_df.columns if c not in existing_cols]
    top_option_trades_df = top_option_trades_df[existing_cols + other_cols]
    top_option_trades_df = sanitize_dataframe_for_streamlit(top_option_trades_df)

    st.dataframe(top_option_trades_df, width="stretch", height=360)
else:
    st.info("No top option prints yet.")

st.divider()

left2, right2 = st.columns(2)

with left2:
    st.subheader("Recent Big Stock Trades")

    if big_trades:
        big_trades_df = pd.DataFrame(big_trades[::-1])

        if "timestamp" in big_trades_df.columns:
            big_trades_df["timestamp"] = big_trades_df["timestamp"].apply(normalize_any_timestamp)

        if "timestamp_str" in big_trades_df.columns:
            big_trades_df["timestamp_str"] = big_trades_df["timestamp_str"].apply(normalize_any_timestamp)

        big_trades_df = sanitize_dataframe_for_streamlit(big_trades_df)
        st.dataframe(big_trades_df, width="stretch", height=280)
    else:
        st.info("No big stock trades yet.")

with right2:
    st.subheader("Recent Option Prints")

    if option_prints:
        option_prints_clean = []
        for x in option_prints[::-1]:
            clean = dict(x)
            row = coerce_trade_row(x)
            clean["timestamp"] = row["timestamp"]
            clean["timestamp_str"] = row["timestamp"]
            clean["expiration"] = row["expiration"]
            clean["option_type"] = row["option_type"]
            clean["strike"] = row["strike"]
            clean["premium"] = row["premium"]
            clean["size"] = row["size"]
            option_prints_clean.append(clean)

        option_prints_df = pd.DataFrame(option_prints_clean)

        preferred_cols = [
            "timestamp",
            "expiration",
            "option_type",
            "strike",
            "premium",
            "size",
            "price",
            "side",
            "option_symbol",
            "underlying",
            "dte",
            "tier",
            "tag",
        ]
        existing_cols = [c for c in preferred_cols if c in option_prints_df.columns]
        other_cols = [c for c in option_prints_df.columns if c not in existing_cols]
        option_prints_df = option_prints_df[existing_cols + other_cols]

        if "timestamp_str" in option_prints_df.columns:
            option_prints_df["timestamp_str"] = option_prints_df["timestamp_str"].apply(normalize_any_timestamp)

        option_prints_df = sanitize_dataframe_for_streamlit(option_prints_df)
        st.dataframe(option_prints_df, width="stretch", height=280)
    else:
        st.info("No option prints yet.")

last_refresh = getattr(data_store, "last_backend_refresh_utc", None)
if last_refresh:
    stamp = normalize_any_timestamp(last_refresh)
    st.caption(
        "Flow Detail using live + sticky last-good backend data. "
        f"Last cache refresh UTC: {stamp}"
    )