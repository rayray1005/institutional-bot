import pandas as pd
import streamlit as st

from config import WATCHLIST
from data_store import get_display_data, last_backend_refresh_utc

st.set_page_config(layout="wide", page_title="Flow Detail")

st.title("Flow Detail")

selected = st.selectbox("Select ticker", WATCHLIST, key="flow_ticker")
data = get_display_data(selected)

# -----------------------------
# CORE DATA
# -----------------------------
spot = data.get("price", 0.0)

buy_flow = data.get("buy_flow", 0.0)
sell_flow = data.get("sell_flow", 0.0)
net_flow = data.get("net_flow", 0.0)

call_premium = data.get("call_premium", 0.0)
put_premium = data.get("put_premium", 0.0)
options_bias = data.get("options_bias", "NEUTRAL")

trade_count = data.get("trade_count", 0)
avg_trade_size = data.get("avg_trade_size", 0.0)
classification_rate = data.get("classification_rate", 0.0)

support = data.get("support", 0.0)
resistance = data.get("resistance", 0.0)

big_trades = data.get("big_trades", [])
option_prints = data.get("option_prints", [])
top_option_trades = data.get("top_option_trades", [])
delta_walls = data.get("delta_walls", [])
gamma_zones = data.get("gamma_zones", [])
tier_stats = data.get("tier_stats", {})
signal_summary = data.get("signal_summary", {})

call_premium_by_strike = data.get("call_premium_by_strike", {})
put_premium_by_strike = data.get("put_premium_by_strike", {})
total_premium_by_strike = data.get("total_premium_by_strike", {})
call_volume_by_strike = data.get("call_volume_by_strike", {})
put_volume_by_strike = data.get("put_volume_by_strike", {})
strike_last_update = data.get("strike_last_update", {})

signal_direction = signal_summary.get("direction", "NEUTRAL")
signal_confidence = signal_summary.get("confidence", 0)
signal_reason = signal_summary.get("reason", "Not enough data yet.")

# -----------------------------
# WALL LOGIC
# -----------------------------
above_wall = None
below_wall = None
strongest_wall = None
nearest_wall = None

if delta_walls:
    strongest_wall = max(delta_walls, key=lambda x: x.get("total_premium", 0.0))
    nearest_wall = min(delta_walls, key=lambda x: abs(x.get("distance_from_spot", 9999)))

    above = [w for w in delta_walls if w.get("strike", 0) > spot]
    below = [w for w in delta_walls if w.get("strike", 0) < spot]

    if above:
        above_wall = min(above, key=lambda x: x.get("strike", 0))
    if below:
        below_wall = max(below, key=lambda x: x.get("strike", 0))


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

        if dist_above <= 2 and dist_below <= 2:
            return {
                "setup": "PIN / SQUEEZE ZONE",
                "trigger": (
                    f"Price trapped between {wall_below['strike']} and "
                    f"{wall_above['strike']}. Watch for break."
                ),
                "risk": "Expect chop until one side clearly breaks.",
            }

        if direction in ["BULLISH", "SLIGHTLY BULLISH"] and dist_above <= 3:
            return {
                "setup": "BULLISH BREAK WATCH",
                "trigger": f"Break and hold above {wall_above['strike']}.",
                "risk": f"Failure back below {wall_above['strike']} can trap buyers.",
            }

        if direction in ["BEARISH", "SLIGHTLY BEARISH"] and dist_below <= 3:
            return {
                "setup": "BEARISH BREAK WATCH",
                "trigger": f"Break and hold below {wall_below['strike']}.",
                "risk": f"Failure back above {wall_below['strike']} can trap sellers.",
            }

        return {
            "setup": "RANGE BETWEEN WALLS",
            "trigger": (
                f"Above {wall_above['strike']} may open upside. "
                f"Below {wall_below['strike']} may open downside."
            ),
            "risk": "Middle of range can stay noisy.",
        }

    if wall_above:
        dist_above = abs(wall_above["strike"] - spot_price)
        if dist_above <= 3:
            return {
                "setup": "TESTING OVERHEAD WALL",
                "trigger": f"Watch reaction at {wall_above['strike']}.",
                "risk": "Rejection can send price back down.",
            }

    if wall_below:
        dist_below = abs(spot_price - wall_below["strike"])
        if dist_below <= 3:
            return {
                "setup": "TESTING SUPPORT WALL",
                "trigger": f"Watch reaction at {wall_below['strike']}.",
                "risk": f"Loss of {wall_below['strike']} can accelerate down.",
            }

    if confidence >= 70:
        return {
            "setup": f"{direction} FLOW BIAS",
            "trigger": "Follow strongest prints and nearest wall reaction.",
            "risk": "Needs confirmation from live price movement.",
        }

    return {
        "setup": "NO CLEAN SETUP",
        "trigger": "Wait for stronger wall alignment or fresh prints.",
        "risk": "Low edge right now.",
    }


trade_setup = build_trade_setup(
    spot,
    above_wall,
    below_wall,
    signal_direction,
    signal_confidence,
)

# -----------------------------
# STRIKE MAP TABLE
# -----------------------------
strike_rows = []

all_strikes = sorted(set(total_premium_by_strike.keys()))
for strike in all_strikes:
    cp = call_premium_by_strike.get(strike, 0.0)
    pp = put_premium_by_strike.get(strike, 0.0)
    tv = total_premium_by_strike.get(strike, 0.0)
    cv = call_volume_by_strike.get(strike, 0)
    pv = put_volume_by_strike.get(strike, 0)
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
)[:12]

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

st.divider()

st.subheader("Key Levels (Live Walls)")

k1, k2, k3, k4 = st.columns(4)

with k1:
    if above_wall:
        st.metric(
            "Wall Above",
            f"{above_wall['strike']}",
            f"{above_wall['bias']} | {above_wall['distance_from_spot']:+}",
        )
    else:
        st.metric("Wall Above", "N/A")

with k2:
    if below_wall:
        st.metric(
            "Wall Below",
            f"{below_wall['strike']}",
            f"{below_wall['bias']} | {below_wall['distance_from_spot']:+}",
        )
    else:
        st.metric("Wall Below", "N/A")

with k3:
    if strongest_wall:
        st.metric(
            "Strongest Wall",
            f"{strongest_wall['strike']}",
            f"${strongest_wall['total_premium']:,.0f}",
        )
    else:
        st.metric("Strongest Wall", "N/A")

with k4:
    if nearest_wall:
        st.metric(
            "Nearest Wall",
            f"{nearest_wall['strike']}",
            f"{nearest_wall['distance_from_spot']:+}",
        )
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
    st.metric("Big Trades", f"{len(list(big_trades)):,}")

with col8:
    st.metric("Option Prints", f"{len(list(option_prints)):,}")

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
            "Premium": round(stats.get("premium", 0.0), 2),
        }
    )

tier_df = pd.DataFrame(tier_rows)
st.dataframe(tier_df, width="stretch", height=180)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Delta Walls")

    if delta_walls:
        delta_walls_df = pd.DataFrame(delta_walls)
        st.dataframe(delta_walls_df, width="stretch", height=260)
    else:
        st.info("No delta walls yet.")

with right:
    st.subheader("Gamma Zones")

    if gamma_zones:
        gamma_zones_df = pd.DataFrame(gamma_zones)
        st.dataframe(gamma_zones_df, width="stretch", height=260)
    else:
        st.info("No gamma zones yet.")

st.divider()

st.subheader("Strike Map Near Spot")

if strike_rows_sorted:
    strike_df = pd.DataFrame(strike_rows_sorted)
    st.dataframe(strike_df, width="stretch", height=320)
else:
    st.info("No strike map data yet.")

st.divider()

st.subheader("Top Option Prints")

if top_option_trades:
    top_option_trades_df = pd.DataFrame(top_option_trades)

    if "timestamp" in top_option_trades_df.columns:
        top_option_trades_df["timestamp"] = top_option_trades_df["timestamp"].astype(str)

    st.dataframe(top_option_trades_df, width="stretch", height=320)
else:
    st.info("No top option prints yet.")

st.divider()

left2, right2 = st.columns(2)

with left2:
    st.subheader("Recent Big Stock Trades")

    big_trades_list = list(big_trades) if hasattr(big_trades, "__iter__") else []

    if big_trades_list:
        big_trades_df = pd.DataFrame(big_trades_list[::-1])

        if "timestamp" in big_trades_df.columns:
            big_trades_df["timestamp"] = big_trades_df["timestamp"].astype(str)

        st.dataframe(big_trades_df, width="stretch", height=260)
    else:
        st.info("No big stock trades yet.")

with right2:
    st.subheader("Recent Option Prints")

    option_prints_list = list(option_prints) if hasattr(option_prints, "__iter__") else []

    if option_prints_list:
        option_prints_df = pd.DataFrame(option_prints_list[::-1])

        if "timestamp" in option_prints_df.columns:
            option_prints_df["timestamp"] = option_prints_df["timestamp"].astype(str)

        st.dataframe(option_prints_df, width="stretch", height=260)
    else:
        st.info("No option prints yet.")

if last_backend_refresh_utc:
    st.caption(
        "Flow Detail using live + sticky last-good backend data. "
        f"Last cache refresh UTC: {last_backend_refresh_utc.strftime('%Y-%m-%d %H:%M:%S')}"
    )