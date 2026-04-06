from datetime import datetime

from data_store import market_data
from config import LARGE_STOCK_PRINT_SIZE


def process_stock_trade(event: dict):
    symbol = event.get("sym") or event.get("symbol")
    if not symbol or symbol not in market_data:
        return

    price = float(event.get("p") or event.get("price") or 0)
    size = int(event.get("s") or event.get("size") or 0)
    timestamp = datetime.now()

    if price <= 0 or size <= 0:
        return

    data = market_data[symbol]

    data["stock_events"] += 1
    data["last_stock_update"] = timestamp.strftime("%H:%M:%S")

    old_price = float(data["price"] or 0.0)
    if old_price <= 0:
        old_price = price

    data["price"] = price
    data["change"] = round(price - old_price, 2)

    data["trade_count"] += 1

    if data["trade_count"] == 1:
        data["avg_trade_size"] = float(size)
    else:
        prev_avg = float(data["avg_trade_size"] or 0.0)
        count = int(data["trade_count"])
        data["avg_trade_size"] = ((prev_avg * (count - 1)) + size) / count

    side = _classify_trade_side(data, price, old_price, size)

    notional = round(price * size, 2)

    if side == "BUY":
        data["buy_flow"] += notional
    elif side == "SELL":
        data["sell_flow"] += notional

    data["net_flow"] = round(data["buy_flow"] - data["sell_flow"], 2)
    data["classification_rate"] = _calc_classification_rate(data)

    if size >= LARGE_STOCK_PRINT_SIZE:
        data["big_trades"].append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "price": price,
                "size": size,
                "side": side,
                "notional": notional,
            }
        )
        data["last_trade"] = f"BIG {symbol} @ ${price:.2f} x {size} [{side}]"

    _update_last_trade_context(data, price, size, side)
    _update_candle(symbol, price, size, timestamp)


def _classify_trade_side(data: dict, price: float, old_price: float, size: int) -> str:
    last_side = data.get("_last_stock_side", "UNKNOWN")
    tick = round(price - old_price, 4)

    if tick > 0:
        return "BUY"
    if tick < 0:
        return "SELL"

    last_big_trades = list(data.get("big_trades", []))
    if last_big_trades:
        most_recent = last_big_trades[-1]
        recent_side = str(most_recent.get("side", "UNKNOWN")).upper()
        if recent_side in ["BUY", "SELL"]:
            return recent_side

    if last_side in ["BUY", "SELL"]:
        return last_side

    avg_trade_size = float(data.get("avg_trade_size", 0.0) or 0.0)
    if avg_trade_size > 0:
        if size > avg_trade_size * 1.5:
            return "BUY"
        if size < avg_trade_size * 0.5:
            return "SELL"

    return "UNKNOWN"


def _calc_classification_rate(data: dict) -> float:
    trade_count = int(data.get("trade_count", 0) or 0)
    if trade_count <= 0:
        return 0.0

    unknown_count = int(data.get("_unknown_trade_count", 0) or 0)
    classified = max(trade_count - unknown_count, 0)
    return round((classified / trade_count) * 100, 2)


def _update_last_trade_context(data: dict, price: float, size: int, side: str):
    data["_last_stock_price"] = price
    data["_last_stock_size"] = size
    data["_last_stock_side"] = side

    if side == "UNKNOWN":
        data["_unknown_trade_count"] = int(data.get("_unknown_trade_count", 0) or 0) + 1


def _update_candle(symbol: str, price: float, size: int, timestamp: datetime):
    data = market_data[symbol]

    if len(data["candles"]) == 0:
        data["candles"].append(
            {
                "timestamp": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
            }
        )
        return

    last_candle = data["candles"][-1]
    last_ts = last_candle.get("timestamp")

    if not isinstance(last_ts, datetime):
        data["candles"].append(
            {
                "timestamp": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
            }
        )
        return

    age = (timestamp - last_ts).total_seconds()

    if age >= 60:
        data["candles"].append(
            {
                "timestamp": timestamp,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
            }
        )
    else:
        last_candle["high"] = max(float(last_candle["high"]), price)
        last_candle["low"] = min(float(last_candle["low"]), price)
        last_candle["close"] = price
        last_candle["volume"] += size