import threading

import pandas as pd
import streamlit as st

from config import WATCHLIST
from data_store import get_display_data, last_backend_refresh_utc
from live_feed_manager import start_live_feed_manager

st.set_page_config(layout="wide", page_title="Institutional Dashboard")

st.title("Institutional Dashboard")
st.subheader("Market Scanner")


# -----------------------------
# START LIVE ENGINE ONCE
# -----------------------------
def ensure_data_engine_running():
    if "engine_started" not in st.session_state:
        st.session_state.engine_started = False

    if not st.session_state.engine_started:
        try:
            thread = threading.Thread(
                target=start_live_feed_manager,
                daemon=True,
                name="dashboard-live-feed-manager",
            )
            thread.start()
            st.session_state.engine_started = True
            print("✅ Dashboard requested live feed manager start.")
        except Exception as e:
            print(f"[Dashboard Engine Start Error] {e}")


ensure_data_engine_running()


# -----------------------------
# HELPERS
# -----------------------------
def flow_color(val):
    if isinstance(val, (int, float)):
        if val > 0:
            return "color: #16a34a; font-weight: 700;"
        if val < 0:
            return "color: #dc2626; font-weight: 700;"
    return "color: #6b7280;"


def confidence_color(val):
    if isinstance(val, (int, float)):
        if val >= 70:
            return "color: #16a34a; font-weight: 700;"
        if val >= 40:
            return "color: #2563eb; font-weight: 700;"
    return "color: #6b7280;"


def normalize_bias_text(value):
    text = str(value or "").strip().upper()

    if "BULL" in text or "CALL" in text or "SUPPORT" in text:
        return "BULLISH"
    if "BEAR" in text or "PUT" in text or "RESIST" in text:
        return "BEARISH"
    return "NEUTRAL"


def nearest_wall_info(walls, price):
    above_wall = None
    below_wall = None
    strongest_wall = None
    nearest_wall = None

    clean_walls = [w for w in walls if float(w.get("strike", 0) or 0) > 0]
    if not clean_walls:
        return above_wall, below_wall, strongest_wall, nearest_wall

    strongest_wall = max(
        clean_walls,
        key=lambda x: float(x.get("total_premium", 0.0) or 0.0),
    )
    nearest_wall = min(
        clean_walls,
        key=lambda x: abs(float(x.get("strike", 0) or 0) - price),
    )

    above = [w for w in clean_walls if float(w.get("strike", 0) or 0) > price]
    below = [w for w in clean_walls if float(w.get("strike", 0) or 0) < price]

    if above:
        above_wall = min(above, key=lambda x: float(x.get("strike", 0) or 0))
    if below:
        below_wall = max(below, key=lambda x: float(x.get("strike", 0) or 0))

    return above_wall, below_wall, strongest_wall, nearest_wall


def get_breakout_state(spot_price, wall_above, wall_below):
    if spot_price <= 0:
        return "WAIT"

    if wall_above:
        above_strike = float(wall_above.get("strike", 0) or 0)
        if spot_price > above_strike:
            return "BREAKOUT"

    if wall_below:
        below_strike = float(wall_below.get("strike", 0) or 0)
        if spot_price < below_strike:
            return "BREAKDOWN"

    return "INSIDE WALLS"


def get_tier_pressure(tier_stats):
    t1 = float(tier_stats.get("tier1", {}).get("premium", 0.0) or 0.0)
    t2 = float(tier_stats.get("tier2", {}).get("premium", 0.0) or 0.0)

    if t1 > t2 * 1.5 and t1 > 0:
        return "TIER 1 DOMINANT"

    if t2 > t1 * 1.5 and t2 > 0:
        return "TIER 2 BUILDING"

    if t1 > 0 or t2 > 0:
        return "BALANCED FLOW"

    return "NO TIER DATA"


def get_price_location(spot_price, wall_above, wall_below):
    if spot_price <= 0:
        return "NO PRICE"

    if wall_above and wall_below:
        above_strike = float(wall_above.get("strike", 0) or 0)
        below_strike = float(wall_below.get("strike", 0) or 0)

        if spot_price > above_strike:
            return "ABOVE WALL"
        if spot_price < below_strike:
            return "BELOW WALL"
        if abs(spot_price - above_strike) <= 1.0:
            return "INTO RESISTANCE"
        if abs(spot_price - below_strike) <= 1.0:
            return "ON SUPPORT"
        return "BETWEEN WALLS"

    if wall_above:
        above_strike = float(wall_above.get("strike", 0) or 0)
        if spot_price > above_strike:
            return "ABOVE WALL"
        if abs(spot_price - above_strike) <= 1.0:
            return "INTO RESISTANCE"
        return "UNDER WALL"

    if wall_below:
        below_strike = float(wall_below.get("strike", 0) or 0)
        if spot_price < below_strike:
            return "BELOW WALL"
        if abs(spot_price - below_strike) <= 1.0:
            return "ON SUPPORT"
        return "ABOVE SUPPORT"

    return "NO WALLS"


def get_wall_pressure(spot_price, wall_above, wall_below):
    if spot_price <= 0:
        return "NEUTRAL"

    if wall_above and wall_below:
        dist_up = abs(float(wall_above.get("strike", 0) or 0) - spot_price)
        dist_down = abs(spot_price - float(wall_below.get("strike", 0) or 0))

        if dist_up < dist_down:
            return "PRESSURE UP"
        if dist_down < dist_up:
            return "PRESSURE DOWN"

    if wall_above:
        return "PRESSURE UP"
    if wall_below:
        return "PRESSURE DOWN"

    return "NO WALL PRESSURE"


def render_status_chip(label, subtext=""):
    label_upper = str(label).upper()

    if "BREAKOUT" in label_upper or "BULL" in label_upper or "SUPPORT" in label_upper:
        bg = "rgba(34,197,94,0.16)"
    elif "BREAKDOWN" in label_upper or "BEAR" in label_upper or "RESISTANCE" in label_upper or "BELOW" in label_upper:
        bg = "rgba(239,68,68,0.16)"
    elif "PRESSURE" in label_upper:
        bg = "rgba(59,130,246,0.16)"
    elif "TIER" in label_upper:
        bg = "rgba(168,85,247,0.16)"
    else:
        bg = "rgba(148,163,184,0.16)"

    st.markdown(
        f"""
        <div style="
            padding:12px 14px;
            border-radius:14px;
            background:{bg};
            border:1px solid rgba(255,255,255,0.08);
            min-height:88px;
            margin-bottom:8px;
        ">
            <div style="font-size:0.8rem;opacity:0.8;">Status</div>
            <div style="font-size:1rem;font-weight:700;">{label}</div>
            <div style="font-size:0.82rem;opacity:0.8;">{subtext}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_scanner_row(symbol, data):
    spot = float(data.get("price", 0.0) or 0.0)
    change = float(data.get("change", 0.0) or 0.0)
    net_flow = float(data.get("net_flow", 0.0) or 0.0)
    options_bias = str(data.get("options_bias", "NEUTRAL") or "NEUTRAL")

    signal_summary = data.get("signal_summary", {}) or {}
    signal_direction = str(signal_summary.get("direction", "NEUTRAL") or "NEUTRAL")
    signal_confidence = int(signal_summary.get("confidence", 0) or 0)

    tier_stats = data.get("tier_stats", {}) or {}
    t1 = float(tier_stats.get("tier1", {}).get("premium", 0.0) or 0.0)
    t2 = float(tier_stats.get("tier2", {}).get("premium", 0.0) or 0.0)

    delta_walls = list(data.get("delta_walls", []) or [])
    above_wall, below_wall, strongest_wall, nearest_wall = nearest_wall_info(delta_walls, spot)

    breakout_state = get_breakout_state(spot, above_wall, below_wall)
    price_location = get_price_location(spot, above_wall, below_wall)

    score = signal_confidence
    score += 8 if abs(net_flow) > 0 else 0
    score += 6 if t1 > 0 else 0
    score += 4 if breakout_state in ["BREAKOUT", "BREAKDOWN"] else 0

    return {
        "Ticker": symbol,
        "Price": round(spot, 2),
        "Change": round(change, 2),
        "Net Flow": round(net_flow, 2),
        "Signal": signal_direction,
        "Confidence": signal_confidence,
        "Options Bias": options_bias,
        "Tier 1": round(t1, 2),
        "Tier 2": round(t2, 2),
        "Breakout": breakout_state,
        "Location": price_location,
        "Nearest Wall": round(float(nearest_wall.get("strike", 0) or 0), 2) if nearest_wall else 0.0,
        "Score": score,
    }


# -----------------------------
# BUILD MARKET SCANNER
# -----------------------------
scanner_rows = []

for symbol in WATCHLIST:
    display_data = get_display_data(symbol)
    scanner_rows.append(build_scanner_row(symbol, display_data))

scanner_df = pd.DataFrame(scanner_rows).sort_values(
    by=["Score", "Confidence", "Net Flow"],
    ascending=[False, False, False],
).reset_index(drop=True)

# -----------------------------
# TOP OVERVIEW
# -----------------------------
bullish_count = int((scanner_df["Signal"].astype(str).str.contains("BULL", case=False)).sum()) if not scanner_df.empty else 0
bearish_count = int((scanner_df["Signal"].astype(str).str.contains("BEAR", case=False)).sum()) if not scanner_df.empty else 0
breakout_count = int((scanner_df["Breakout"] == "BREAKOUT").sum()) if not scanner_df.empty else 0
breakdown_count = int((scanner_df["Breakout"] == "BREAKDOWN").sum()) if not scanner_df.empty else 0

o1, o2, o3, o4, o5 = st.columns(5)

with o1:
    st.metric("Watchlist Names", len(WATCHLIST))
with o2:
    st.metric("Bullish Signals", bullish_count)
with o3:
    st.metric("Bearish Signals", bearish_count)
with o4:
    st.metric("Breakouts", breakout_count)
with o5:
    st.metric("Breakdowns", breakdown_count)

st.divider()

# -----------------------------
# SCANNER TABLE
# -----------------------------
st.subheader("Ranked Watchlist Scanner")

st.dataframe(
    scanner_df.style.map(flow_color, subset=["Net Flow"]).map(confidence_color, subset=["Confidence"]),
    width="stretch",
    height=420,
)

st.divider()

# -----------------------------
# SELECTED TICKER COMMAND CENTER
# -----------------------------
selected = st.selectbox("Command Center Ticker", WATCHLIST, key="dashboard_ticker")
data = get_display_data(selected)

spot = float(data.get("price", 0.0) or 0.0)
change = float(data.get("change", 0.0) or 0.0)
net_flow = float(data.get("net_flow", 0.0) or 0.0)
buy_flow = float(data.get("buy_flow", 0.0) or 0.0)
sell_flow = float(data.get("sell_flow", 0.0) or 0.0)
call_premium = float(data.get("call_premium", 0.0) or 0.0)
put_premium = float(data.get("put_premium", 0.0) or 0.0)
options_bias = str(data.get("options_bias", "NEUTRAL") or "NEUTRAL")
support = float(data.get("support", 0.0) or 0.0)
resistance = float(data.get("resistance", 0.0) or 0.0)
trade_count = int(data.get("trade_count", 0) or 0)
avg_trade_size = float(data.get("avg_trade_size", 0.0) or 0.0)
classification_rate = float(data.get("classification_rate", 0.0) or 0.0)

signal_summary = data.get("signal_summary", {}) or {}
signal_direction = str(signal_summary.get("direction", "NEUTRAL") or "NEUTRAL")
signal_confidence = int(signal_summary.get("confidence", 0) or 0)
signal_reason = str(signal_summary.get("reason", "Not enough data yet.") or "Not enough data yet.")
setup = str(signal_summary.get("setup", signal_direction) or signal_direction)
trigger = str(signal_summary.get("trigger", signal_reason) or signal_reason)

tier_stats = data.get("tier_stats", {}) or {}
t1 = float(tier_stats.get("tier1", {}).get("premium", 0.0) or 0.0)
t2 = float(tier_stats.get("tier2", {}).get("premium", 0.0) or 0.0)
other_tier = float(tier_stats.get("other", {}).get("premium", 0.0) or 0.0)

delta_walls = list(data.get("delta_walls", []) or [])
above_wall, below_wall, strongest_wall, nearest_wall = nearest_wall_info(delta_walls, spot)

breakout_state = get_breakout_state(spot, above_wall, below_wall)
wall_pressure = get_wall_pressure(spot, above_wall, below_wall)
tier_pressure = get_tier_pressure(tier_stats)
price_location = get_price_location(spot, above_wall, below_wall)

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("Price", f"{spot:,.2f}", f"{change:,.2f}")
with c2:
    st.metric("Net Flow", f"{net_flow:,.2f}")
with c3:
    st.metric("Call Premium", f"{call_premium:,.2f}")
with c4:
    st.metric("Put Premium", f"{put_premium:,.2f}")

st.write(f"**Institutional Thesis:** {signal_reason}")
st.write(f"**Setup:** {setup}")
st.write(f"**Trigger:** {trigger}")

st.subheader("Live Status")

s1, s2, s3, s4, s5 = st.columns(5)

with s1:
    render_status_chip(breakout_state, f"Spot {spot:.2f}")

with s2:
    wall_sub = "Wall structure still building"
    if above_wall and below_wall:
        wall_sub = (
            f"Above {float(above_wall.get('strike', 0) or 0):.2f} | "
            f"Below {float(below_wall.get('strike', 0) or 0):.2f}"
        )
    render_status_chip(wall_pressure, wall_sub)

with s3:
    render_status_chip(
        f"{normalize_bias_text(options_bias)} OPTIONS FLOW",
        f"Signal {signal_direction} | {signal_confidence}%",
    )

with s4:
    render_status_chip(tier_pressure, f"T1 ${t1:,.0f} | T2 ${t2:,.0f}")

with s5:
    render_status_chip(
        price_location,
        f"Nearest wall {float(nearest_wall.get('strike', 0) or 0):.2f}" if nearest_wall else "No wall detected",
    )

st.divider()

# -----------------------------
# COMMAND CENTER DETAILS
# -----------------------------
d1, d2, d3, d4, d5, d6 = st.columns(6)

with d1:
    st.metric("Signal", signal_direction)
with d2:
    st.metric("Confidence", f"{signal_confidence}%")
with d3:
    st.metric("Support", f"{support:,.2f}")
with d4:
    st.metric("Resistance", f"{resistance:,.2f}")
with d5:
    st.metric("Range Width", f"{(resistance - support):,.2f}" if support > 0 and resistance > 0 else "0.00")
with d6:
    st.metric("Nearest Wall", f"{float(nearest_wall.get('strike', 0) or 0):.2f}" if nearest_wall else "N/A")

d7, d8, d9, d10, d11, d12 = st.columns(6)

with d7:
    st.metric("Buy Flow", f"{buy_flow:,.2f}")
with d8:
    st.metric("Sell Flow", f"{sell_flow:,.2f}")
with d9:
    st.metric("Trades", f"{trade_count:,}")
with d10:
    st.metric("Avg Trade Size", f"{avg_trade_size:,.2f}")
with d11:
    st.metric("Class Rate", f"{classification_rate:,.2f}%")
with d12:
    st.metric("Strongest Wall", f"{float(strongest_wall.get('strike', 0) or 0):.2f}" if strongest_wall else "N/A")

st.divider()

# -----------------------------
# TIER + WALLS TABLES
# -----------------------------
left, right = st.columns(2)

with left:
    st.subheader("Tier Breakdown")
    tier_rows = [
        {"Tier": "TIER 1", "Premium": round(t1, 2)},
        {"Tier": "TIER 2", "Premium": round(t2, 2)},
        {"Tier": "OTHER", "Premium": round(other_tier, 2)},
    ]
    st.dataframe(pd.DataFrame(tier_rows), width="stretch", height=180)

with right:
    st.subheader("Wall Levels")
    wall_rows = []

    if above_wall:
        wall_rows.append(
            {
                "Type": "Wall Above",
                "Strike": round(float(above_wall.get("strike", 0) or 0), 2),
                "Bias": above_wall.get("bias", ""),
                "Premium": round(float(above_wall.get("total_premium", 0) or 0), 2),
            }
        )

    if below_wall:
        wall_rows.append(
            {
                "Type": "Wall Below",
                "Strike": round(float(below_wall.get("strike", 0) or 0), 2),
                "Bias": below_wall.get("bias", ""),
                "Premium": round(float(below_wall.get("total_premium", 0) or 0), 2),
            }
        )

    if strongest_wall:
        wall_rows.append(
            {
                "Type": "Strongest Wall",
                "Strike": round(float(strongest_wall.get("strike", 0) or 0), 2),
                "Bias": strongest_wall.get("bias", ""),
                "Premium": round(float(strongest_wall.get("total_premium", 0) or 0), 2),
            }
        )

    if nearest_wall:
        wall_rows.append(
            {
                "Type": "Nearest Wall",
                "Strike": round(float(nearest_wall.get("strike", 0) or 0), 2),
                "Bias": nearest_wall.get("bias", ""),
                "Premium": round(float(nearest_wall.get("total_premium", 0) or 0), 2),
            }
        )

    if wall_rows:
        st.dataframe(pd.DataFrame(wall_rows), width="stretch", height=180)
    else:
        st.info("No wall data yet.")

st.divider()

# -----------------------------
# FEED MONITOR
# -----------------------------
st.subheader("Feed Monitor")

status_rows = []

for symbol in WATCHLIST:
    d = get_display_data(symbol)
    status_rows.append(
        {
            "Ticker": symbol,
            "Stock Events": d.get("stock_events", 0),
            "Option Events": d.get("option_events", 0),
            "Last Stock Update": d.get("last_stock_update", ""),
            "Last Option Update": d.get("last_option_update", ""),
            "Last Trade": d.get("last_trade", ""),
        }
    )

status_df = pd.DataFrame(status_rows)
st.dataframe(status_df, width="stretch", height=280)

if last_backend_refresh_utc:
    st.caption(
        "Dashboard using live + sticky last-good backend data. "
        f"Last cache refresh UTC: {last_backend_refresh_utc.strftime('%Y-%m-%d %H:%M:%S')}"
    )