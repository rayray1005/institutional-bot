import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import WATCHLIST
from data_store import get_display_data, last_backend_refresh_utc

st.set_page_config(layout="wide", page_title="Chart")

st.title("Institutional Chart")

selected = st.selectbox("Select ticker", WATCHLIST, key="chart_ticker")
timeframe = st.selectbox("Timeframe", ["1m", "5m", "15m", "1h", "4h", "1D"], index=1)

data = get_display_data(selected)

spot = float(data.get("price", 0.0) or 0.0)
support = float(data.get("support", 0.0) or 0.0)
resistance = float(data.get("resistance", 0.0) or 0.0)

delta_walls = list(data.get("delta_walls", []) or [])
gamma_zones = list(data.get("gamma_zones", []) or [])
big_trades = list(data.get("big_trades", []) or [])
signal_summary = data.get("signal_summary", {}) or {}
options_bias = str(data.get("options_bias", "NEUTRAL") or "NEUTRAL")
tier_stats = data.get("tier_stats", {}) or {}

signal_direction = str(signal_summary.get("direction", "NEUTRAL") or "NEUTRAL")
signal_confidence = int(signal_summary.get("confidence", 0) or 0)
signal_reason = str(signal_summary.get("reason", "Not enough data yet.") or "Not enough data yet.")


def candles_to_df(candles):
    candles_list = list(candles) if hasattr(candles, "__iter__") else []
    if not candles_list:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(candles_list)
    if df.empty:
        return df

    required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in df.columns:
            if col == "timestamp":
                df[col] = pd.NaT
            else:
                df[col] = 0.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def resample_candles(df, tf):
    if df.empty:
        return df

    rule_map = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1H",
        "4h": "4H",
        "1D": "1D",
    }

    rule = rule_map.get(tf, "5min")

    resampled = (
        df.set_index("timestamp")
        .resample(rule)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )

    return resampled


def build_big_trade_markers(big_trade_rows):
    buy_x, buy_y = [], []
    sell_x, sell_y = [], []
    unknown_x, unknown_y = [], []

    for trade in big_trade_rows:
        ts = pd.to_datetime(trade.get("timestamp"), errors="coerce")
        px = float(trade.get("price", 0.0) or 0.0)
        side = str(trade.get("side", "unknown") or "unknown").upper()

        if pd.isna(ts) or px <= 0:
            continue

        if side == "BUY":
            buy_x.append(ts)
            buy_y.append(px)
        elif side == "SELL":
            sell_x.append(ts)
            sell_y.append(px)
        else:
            unknown_x.append(ts)
            unknown_y.append(px)

    return {
        "buy": (buy_x, buy_y),
        "sell": (sell_x, sell_y),
        "unknown": (unknown_x, unknown_y),
    }


def nearest_wall_info(walls, price):
    above_wall = None
    below_wall = None
    strongest_wall = None
    nearest_wall = None

    clean_walls = [w for w in walls if float(w.get("strike", 0) or 0) > 0]
    if not clean_walls:
        return above_wall, below_wall, strongest_wall, nearest_wall

    strongest_wall = max(clean_walls, key=lambda x: float(x.get("total_premium", 0.0) or 0.0))
    nearest_wall = min(clean_walls, key=lambda x: abs(float(x.get("strike", 0) or 0) - price))

    above = [w for w in clean_walls if float(w.get("strike", 0) or 0) > price]
    below = [w for w in clean_walls if float(w.get("strike", 0) or 0) < price]

    if above:
        above_wall = min(above, key=lambda x: float(x.get("strike", 0) or 0))
    if below:
        below_wall = max(below, key=lambda x: float(x.get("strike", 0) or 0))

    return above_wall, below_wall, strongest_wall, nearest_wall


def select_clean_walls(walls, price):
    clean_walls = [w for w in walls if float(w.get("strike", 0) or 0) > 0]
    if not clean_walls:
        return []

    nearest = sorted(
        clean_walls,
        key=lambda x: (
            abs(float(x.get("strike", 0) or 0) - price),
            -float(x.get("total_premium", 0.0) or 0.0),
        ),
    )[:2]

    strongest = sorted(
        clean_walls,
        key=lambda x: float(x.get("total_premium", 0.0) or 0.0),
        reverse=True,
    )[:2]

    seen = set()
    result = []
    for wall in nearest + strongest:
        strike = float(wall.get("strike", 0) or 0)
        if strike not in seen:
            seen.add(strike)
            result.append(wall)

    return result[:4]


def select_clean_gamma_zones(zones, price):
    clean_zones = [z for z in zones if float(z.get("strike", 0) or 0) > 0]
    if not clean_zones:
        return []

    nearest = sorted(
        clean_zones,
        key=lambda x: (
            abs(float(x.get("strike", 0) or 0) - price),
            -float(x.get("zone_strength", 0.0) or 0.0),
        ),
    )[:2]

    strongest = sorted(
        clean_zones,
        key=lambda x: float(x.get("zone_strength", 0.0) or 0.0),
        reverse=True,
    )[:2]

    seen = set()
    result = []
    for zone in nearest + strongest:
        strike = float(zone.get("strike", 0) or 0)
        if strike not in seen:
            seen.add(strike)
            result.append(zone)

    return result[:4]


def build_trade_plan(spot_price, wall_above, wall_below, direction):
    if spot_price <= 0:
        return "No live price yet."

    if wall_above and wall_below:
        return (
            f"Above {float(wall_above['strike']):.2f} = upside continuation watch. "
            f"Below {float(wall_below['strike']):.2f} = downside acceleration watch."
        )

    if wall_above:
        return f"Watch reaction into overhead wall at {float(wall_above['strike']):.2f}."

    if wall_below:
        return f"Watch reaction into support wall at {float(wall_below['strike']):.2f}."

    return f"{direction} bias, but no clean wall structure yet."


def normalize_bias_text(value):
    text = str(value or "").strip().upper()

    if "BULL" in text or "CALL" in text or "SUPPORT" in text:
        return "BULLISH"
    if "BEAR" in text or "PUT" in text or "RESIST" in text:
        return "BEARISH"
    return "NEUTRAL"


def get_wall_pressure(spot_price, wall_above, wall_below):
    if spot_price <= 0:
        return "NEUTRAL"

    if wall_above and wall_below:
        dist_up = abs(float(wall_above.get("strike", 0) or 0) - spot_price)
        dist_down = abs(spot_price - float(wall_below.get("strike", 0) or 0))
        above_bias = normalize_bias_text(wall_above.get("bias", ""))
        below_bias = normalize_bias_text(wall_below.get("bias", ""))

        if dist_up < dist_down:
            return f"PRESSURE INTO {above_bias} WALL"
        if dist_down < dist_up:
            return f"PRESSURE INTO {below_bias} WALL"

    if wall_above:
        return f"PRESSURE INTO {normalize_bias_text(wall_above.get('bias', ''))} WALL"
    if wall_below:
        return f"PRESSURE INTO {normalize_bias_text(wall_below.get('bias', ''))} WALL"

    return "NO WALL PRESSURE"


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
        return "TIER 1 DOMINANT", f"T1 ${t1:,.0f} vs T2 ${t2:,.0f}"

    if t2 > t1 * 1.5 and t2 > 0:
        return "TIER 2 BUILDING", f"T2 ${t2:,.0f} vs T1 ${t1:,.0f}"

    if t1 > 0 or t2 > 0:
        return "BALANCED FLOW", f"T1 ${t1:,.0f} | T2 ${t2:,.0f}"

    return "NO TIER DATA", "Waiting for tier prints"


def get_price_location(spot_price, wall_above, wall_below):
    if spot_price <= 0:
        return "NO PRICE", "Waiting for live price"

    if wall_above and wall_below:
        above_strike = float(wall_above.get("strike", 0) or 0)
        below_strike = float(wall_below.get("strike", 0) or 0)

        if spot_price > above_strike:
            return "TRADING ABOVE WALL", f"Above {above_strike:.2f}"
        if spot_price < below_strike:
            return "TRADING BELOW WALL", f"Below {below_strike:.2f}"

        mid = (above_strike + below_strike) / 2
        if abs(spot_price - above_strike) <= 1.0:
            return "TRADING INTO RESISTANCE", f"Near {above_strike:.2f}"
        if abs(spot_price - below_strike) <= 1.0:
            return "TRADING ON SUPPORT", f"Near {below_strike:.2f}"

        if spot_price >= mid:
            return "UPPER RANGE", f"{below_strike:.2f} to {above_strike:.2f}"
        return "LOWER RANGE", f"{below_strike:.2f} to {above_strike:.2f}"

    if wall_above:
        above_strike = float(wall_above.get("strike", 0) or 0)
        if abs(spot_price - above_strike) <= 1.0:
            return "TRADING INTO RESISTANCE", f"Near {above_strike:.2f}"
        if spot_price > above_strike:
            return "TRADING ABOVE WALL", f"Above {above_strike:.2f}"
        return "TRADING UNDER WALL", f"Under {above_strike:.2f}"

    if wall_below:
        below_strike = float(wall_below.get("strike", 0) or 0)
        if abs(spot_price - below_strike) <= 1.0:
            return "TRADING ON SUPPORT", f"Near {below_strike:.2f}"
        if spot_price < below_strike:
            return "TRADING BELOW WALL", f"Below {below_strike:.2f}"
        return "TRADING ABOVE SUPPORT", f"Above {below_strike:.2f}"

    return "NO WALL LOCATION", f"Spot {spot_price:.2f}"


def get_chip_bg(label):
    label = str(label).upper()

    if "BREAKOUT" in label or "BULL" in label or "UPSIDE" in label or "SUPPORT" in label:
        return "rgba(34,197,94,0.18)"
    if "BREAKDOWN" in label or "BEAR" in label or "DOWNSIDE" in label or "RESISTANCE" in label or "BELOW" in label:
        return "rgba(239,68,68,0.18)"
    if "NEUTRAL" in label or "INSIDE" in label or "WAIT" in label or "BALANCED" in label:
        return "rgba(148,163,184,0.18)"
    if "PRESSURE" in label or "UPPER RANGE" in label or "LOWER RANGE" in label:
        return "rgba(59,130,246,0.18)"
    if "TIER" in label:
        return "rgba(168,85,247,0.18)"
    return "rgba(168,85,247,0.18)"


def render_status_chip(label, subtext=""):
    bg = get_chip_bg(label)
    text = f"""
    <div style="
        padding: 12px 14px;
        border-radius: 14px;
        background: {bg};
        border: 1px solid rgba(255,255,255,0.08);
        margin-bottom: 8px;
        min-height: 90px;
    ">
        <div style="font-size: 0.82rem; opacity: 0.80;">Status</div>
        <div style="font-size: 1rem; font-weight: 700;">{label}</div>
        <div style="font-size: 0.82rem; opacity: 0.80;">{subtext}</div>
    </div>
    """
    st.markdown(text, unsafe_allow_html=True)


def price_band_halfwidth(strike):
    return max(strike * 0.0012, 0.4)


candles_df = candles_to_df(data.get("candles", []))
chart_df = resample_candles(candles_df, timeframe)

above_wall, below_wall, strongest_wall, nearest_wall = nearest_wall_info(delta_walls, spot)
plot_walls = select_clean_walls(delta_walls, spot)
plot_gamma_zones = select_clean_gamma_zones(gamma_zones, spot)

trade_plan = build_trade_plan(spot, above_wall, below_wall, signal_direction)
wall_pressure = get_wall_pressure(spot, above_wall, below_wall)
breakout_state = get_breakout_state(spot, above_wall, below_wall)
tier_label, tier_sub = get_tier_pressure(tier_stats)
price_location_label, price_location_sub = get_price_location(spot, above_wall, below_wall)

setup = signal_summary.get("setup", signal_direction)
trigger = signal_summary.get("reason", "Watching levels")

# -----------------------------
# HEADER METRICS
# -----------------------------
m1, m2, m3, m4, m5 = st.columns(5)

with m1:
    st.metric("Spot", f"{spot:,.2f}")

with m2:
    st.metric("Signal", signal_direction)

with m3:
    st.metric("Confidence", f"{signal_confidence}%")

with m4:
    st.metric("Options Bias", options_bias)

with m5:
    if strongest_wall:
        st.metric(
            "Strongest Wall",
            f"{float(strongest_wall['strike']):.2f}",
            f"${float(strongest_wall.get('total_premium', 0.0) or 0.0):,.0f}",
        )
    else:
        st.metric("Strongest Wall", "N/A")

st.write(f"**Reason:** {signal_reason}")
st.write(f"**Setup:** {setup}")
st.write(f"**Trigger:** {trigger}")

# -----------------------------
# STATUS CHIPS
# -----------------------------
st.subheader("Live Status")

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    render_status_chip(
        breakout_state,
        f"Spot {spot:.2f}",
    )

with c2:
    render_status_chip(
        wall_pressure,
        (
            f"Above: {float(above_wall.get('strike', 0) or 0):.2f} | "
            f"Below: {float(below_wall.get('strike', 0) or 0):.2f}"
        ) if (above_wall and below_wall) else "Wall structure still building",
    )

with c3:
    render_status_chip(
        f"{normalize_bias_text(options_bias)} OPTIONS FLOW",
        f"Signal {signal_direction} | Confidence {signal_confidence}%",
    )

with c4:
    render_status_chip(tier_label, tier_sub)

with c5:
    render_status_chip(price_location_label, price_location_sub)

st.divider()

# -----------------------------
# LIVE WALL SUMMARY
# -----------------------------
st.subheader("Live Wall Summary")

w1, w2, w3, w4 = st.columns(4)

with w1:
    if above_wall:
        st.metric(
            "Wall Above",
            f"{float(above_wall['strike']):.2f}",
            f"{above_wall.get('bias', 'N/A')} | {float(above_wall.get('strike', 0) or 0) - spot:+.2f}",
        )
    else:
        st.metric("Wall Above", "N/A")

with w2:
    if below_wall:
        st.metric(
            "Wall Below",
            f"{float(below_wall['strike']):.2f}",
            f"{below_wall.get('bias', 'N/A')} | {spot - float(below_wall.get('strike', 0) or 0):+.2f}",
        )
    else:
        st.metric("Wall Below", "N/A")

with w3:
    if nearest_wall:
        nearest_dist = float(nearest_wall.get("strike", 0) or 0) - spot
        st.metric(
            "Nearest Wall",
            f"{float(nearest_wall['strike']):.2f}",
            f"{nearest_dist:+.2f}",
        )
    else:
        st.metric("Nearest Wall", "N/A")

with w4:
    if support > 0 and resistance > 0:
        st.metric("Range", f"{support:.2f} - {resistance:.2f}", f"{(resistance - support):.2f}")
    else:
        st.metric("Range", "N/A")

st.write(f"**Trade Plan:** {trade_plan}")

st.divider()

# -----------------------------
# CHART
# -----------------------------
st.subheader("Price Chart")

if chart_df.empty:
    st.info("No candle data yet.")
else:
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=chart_df["timestamp"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            name="Price",
        )
    )

    last_ts = chart_df["timestamp"].iloc[-1]

    if spot > 0:
        fig.add_hline(
            y=spot,
            line_width=2,
            line_dash="dot",
            annotation_text=f"Spot {spot:.2f}",
            annotation_position="top left",
        )

    if support > 0:
        fig.add_hline(
            y=support,
            line_width=1,
            line_dash="dash",
            annotation_text=f"Support {support:.2f}",
            annotation_position="bottom left",
        )

    if resistance > 0:
        fig.add_hline(
            y=resistance,
            line_width=1,
            line_dash="dash",
            annotation_text=f"Resistance {resistance:.2f}",
            annotation_position="top left",
        )

    for wall in plot_walls:
        strike = float(wall.get("strike", 0.0) or 0.0)
        bias = str(wall.get("bias", "WALL") or "WALL")
        total_premium = float(wall.get("total_premium", 0.0) or 0.0)

        if strike > 0:
            is_key = (
                (nearest_wall and float(nearest_wall.get("strike", 0) or 0) == strike) or
                (strongest_wall and float(strongest_wall.get("strike", 0) or 0) == strike)
            )

            fig.add_hline(
                y=strike,
                line_width=2.5 if is_key else 1.2,
                line_dash="dot",
                annotation_text=f"{bias} {strike:.2f} | ${total_premium:,.0f}",
                annotation_position="right",
            )

    for zone in plot_gamma_zones:
        strike = float(zone.get("strike", 0.0) or 0.0)
        strength = float(zone.get("zone_strength", 0.0) or 0.0)

        if strike > 0:
            band_half = price_band_halfwidth(strike)
            fig.add_hrect(
                y0=strike - band_half,
                y1=strike + band_half,
                line_width=0,
                annotation_text=f"Gamma {strike:.2f}",
                annotation_position="top left",
                opacity=0.10 if strength <= 0 else min(0.22, max(0.08, strength / 100000)),
            )

    if spot > 0 and above_wall:
        above_strike = float(above_wall.get("strike", 0) or 0)
        if spot > above_strike:
            fig.add_annotation(
                x=last_ts,
                y=spot,
                text=f"BREAKOUT > {above_strike:.2f}",
                showarrow=True,
                arrowhead=2,
                ax=-90,
                ay=-40,
                bgcolor="rgba(34,197,94,0.20)",
                borderpad=6,
            )

    if spot > 0 and below_wall:
        below_strike = float(below_wall.get("strike", 0) or 0)
        if spot < below_strike:
            fig.add_annotation(
                x=last_ts,
                y=spot,
                text=f"BREAKDOWN < {below_strike:.2f}",
                showarrow=True,
                arrowhead=2,
                ax=-90,
                ay=40,
                bgcolor="rgba(239,68,68,0.20)",
                borderpad=6,
            )

    if spot > 0 and above_wall and below_wall:
        above_strike = float(above_wall.get("strike", 0) or 0)
        below_strike = float(below_wall.get("strike", 0) or 0)
        if below_strike < spot < above_strike:
            fig.add_annotation(
                x=last_ts,
                y=spot,
                text=f"INSIDE WALLS {below_strike:.2f} - {above_strike:.2f}",
                showarrow=False,
                yshift=26,
                bgcolor="rgba(148,163,184,0.15)",
                borderpad=6,
            )

    if spot > 0 and nearest_wall:
        nearest_strike = float(nearest_wall.get("strike", 0) or 0)
        direction_text = "Pressure Up" if nearest_strike > spot else "Pressure Down"
        fig.add_annotation(
            x=last_ts,
            y=nearest_strike,
            text=f"{direction_text} → {nearest_strike:.2f}",
            showarrow=True,
            arrowhead=1,
            ax=-80,
            ay=-20 if nearest_strike > spot else 20,
            bgcolor="rgba(59,130,246,0.16)",
            borderpad=5,
        )

    markers = build_big_trade_markers(big_trades)

    buy_x, buy_y = markers["buy"]
    if buy_x:
        fig.add_trace(
            go.Scatter(
                x=buy_x,
                y=buy_y,
                mode="markers",
                name="Big Buy",
                marker=dict(symbol="triangle-up", size=10),
            )
        )

    sell_x, sell_y = markers["sell"]
    if sell_x:
        fig.add_trace(
            go.Scatter(
                x=sell_x,
                y=sell_y,
                mode="markers",
                name="Big Sell",
                marker=dict(symbol="triangle-down", size=10),
            )
        )

    unknown_x, unknown_y = markers["unknown"]
    if unknown_x:
        fig.add_trace(
            go.Scatter(
                x=unknown_x,
                y=unknown_y,
                mode="markers",
                name="Big Unknown",
                marker=dict(symbol="circle", size=8),
            )
        )

    fig.update_layout(
        height=720,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h"),
        margin=dict(l=20, r=20, t=40, b=20),
    )

    st.plotly_chart(fig, width="stretch")

st.divider()

# -----------------------------
# TABLES UNDER CHART
# -----------------------------
left, right = st.columns(2)

with left:
    st.subheader("Key Delta Walls")

    if plot_walls:
        delta_walls_df = pd.DataFrame(plot_walls).copy()
        st.dataframe(delta_walls_df, width="stretch", height=230)
    else:
        st.info("No delta walls yet.")

with right:
    st.subheader("Key Gamma Zones")

    if plot_gamma_zones:
        gamma_zones_df = pd.DataFrame(plot_gamma_zones).copy()
        st.dataframe(gamma_zones_df, width="stretch", height=230)
    else:
        st.info("No gamma zones yet.")

st.divider()

left2, right2 = st.columns(2)

with left2:
    st.subheader("Recent Big Stock Trades")

    if big_trades:
        big_trades_df = pd.DataFrame(big_trades[::-1]).copy()

        if "timestamp" in big_trades_df.columns:
            big_trades_df["timestamp"] = big_trades_df["timestamp"].astype(str)

        st.dataframe(big_trades_df, width="stretch", height=240)
    else:
        st.info("No big stock trades yet.")

with right2:
    st.subheader("Recent Candles")

    if not chart_df.empty:
        recent_candles_df = chart_df.tail(20).copy()
        recent_candles_df["timestamp"] = recent_candles_df["timestamp"].astype(str)
        st.dataframe(recent_candles_df, width="stretch", height=240)
    else:
        st.info("No candle data yet.")

if last_backend_refresh_utc:
    st.caption(
        "Chart using live + sticky last-good backend data. "
        f"Last cache refresh UTC: {last_backend_refresh_utc.strftime('%Y-%m-%d %H:%M:%S')}"
    )