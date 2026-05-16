from flask import Flask, jsonify
from flask_cors import CORS

import asyncio
import threading
import json
import websockets
import requests
import time
import csv
import os
import statistics

from collections import deque
from datetime import datetime

app = Flask(__name__)
CORS(app)

START_USDT = 1000
FEE_RATE = 0.001
SLIPPAGE_RATE = 0.0005
MIN_LIQUIDITY = 1

MAX_TRIANGLES = 20
SCAN_INTERVAL = 1

DEPTH_LEVELS = 10
DEPTH_LOOP_INTERVAL = 3

VOLATILITY_WINDOW = 30
CSV_FILE = "opportunity_logs.csv"

market_data = {}
depth_cache = {}
price_history = {}
route_stats = {}

last_good_opportunities = []
history = []
scanner_cycle_ms = 0

bot_data = {
    "status": "STARTING",
    "last_update": "Waiting...",
    "top_opportunities": [],
    "history": []
}

data_lock = threading.Lock()
depth_lock = threading.Lock()

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "time",
            "route",
            "fast_profit",
            "depth_profit",
            "liquidity",
            "avg_volatility",
            "avg_imbalance",
            "reliability_score",
            "spread_percent",
            "market_latency_ms",
            "depth_latency_ms",
            "scanner_cycle_ms",
            "execution_cost",
            "adjusted_profit",
            "is_profitable"
        ])


def get_triangles():
    MAJOR_COINS = {
        "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"
    }

    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = requests.get(url, timeout=10).json()
    symbols = data["symbols"]

    symbol_names = set()

    for s in symbols:
        if s["status"] == "TRADING":
            symbol_names.add(s["symbol"])

    triangles = []

    for s1 in symbols:
        if s1["status"] != "TRADING":
            continue

        base1 = s1["baseAsset"]
        quote1 = s1["quoteAsset"]
        pair1 = s1["symbol"]

        if quote1 != "USDT":
            continue

        if base1 not in MAJOR_COINS:
            continue

        for s2 in symbols:
            if s2["status"] != "TRADING":
                continue

            base2 = s2["baseAsset"]
            quote2 = s2["quoteAsset"]
            pair2 = s2["symbol"]

            if quote2 != base1:
                continue

            if base2 not in MAJOR_COINS:
                continue

            final_pair = f"{base2}USDT"

            if final_pair in symbol_names:
                triangles.append({
                    "name": f"USDT -> {base1} -> {base2} -> USDT",
                    "pairs": [pair1, pair2, final_pair]
                })

    return triangles[:MAX_TRIANGLES]


try:
    TRIANGLES = get_triangles()
except Exception as e:
    print("Triangle loading failed:", e)
    TRIANGLES = []

SYMBOLS = set()

for triangle in TRIANGLES:
    for pair in triangle["pairs"]:
        SYMBOLS.add(pair)

print(f"Loaded {len(TRIANGLES)} triangles")
print(f"Streaming {len(SYMBOLS)} symbols")


def update_price_history(symbol, price):
    if symbol not in price_history:
        price_history[symbol] = deque(maxlen=VOLATILITY_WINDOW)

    price_history[symbol].append(price)


def calculate_volatility(symbol):
    if symbol not in price_history:
        return 0

    prices = list(price_history[symbol])

    if len(prices) < 5:
        return 0

    try:
        mean_price = statistics.mean(prices)

        if mean_price == 0:
            return 0

        std_dev = statistics.stdev(prices)
        volatility_percent = (std_dev / mean_price) * 100

        return round(volatility_percent, 4)

    except Exception:
        return 0


def calculate_orderbook_imbalance(symbol):
    with depth_lock:
        depth = depth_cache.get(symbol)

    if not depth:
        return 1

    try:
        bids = depth["bids"]
        asks = depth["asks"]

        total_bid_volume = sum(qty for price, qty in bids)
        total_ask_volume = sum(qty for price, qty in asks)

        if total_ask_volume == 0:
            return 1

        imbalance = total_bid_volume / total_ask_volume

        return round(imbalance, 4)

    except Exception:
        return 1


def update_route_stats(route, depth_profit, liquidity, avg_volatility):
    if route not in route_stats:
        route_stats[route] = {
            "seen": 0,
            "profitable": 0,
            "total_liquidity": 0,
            "total_volatility": 0
        }

    stats = route_stats[route]

    stats["seen"] += 1
    stats["total_liquidity"] += liquidity
    stats["total_volatility"] += avg_volatility

    if isinstance(depth_profit, float) and depth_profit > 0:
        stats["profitable"] += 1


def calculate_reliability_score(route):
    if route not in route_stats:
        return 0

    stats = route_stats[route]
    seen = stats["seen"]

    if seen == 0:
        return 0

    profitable_ratio = stats["profitable"] / seen
    avg_liquidity = stats["total_liquidity"] / seen
    avg_volatility = stats["total_volatility"] / seen

    score = 0

    score += min(profitable_ratio * 50, 50)
    score += min(avg_liquidity / 20 * 30, 30)

    volatility_penalty = min(avg_volatility * 10, 20)

    score += max(20 - volatility_penalty, 0)

    return round(score, 2)


def calculate_execution_cost(
    depth_profit,
    liquidity,
    avg_volatility,
    avg_imbalance
):
    if not isinstance(depth_profit, float):
        return 0

    base_cost = START_USDT * (FEE_RATE + SLIPPAGE_RATE)

    volatility_cost = START_USDT * (avg_volatility / 100) * 0.2

    liquidity_cost = 0

    if liquidity < 5:
        liquidity_cost = 2
    elif liquidity < 10:
        liquidity_cost = 1

    imbalance_cost = abs(avg_imbalance - 1) * 0.5

    total_cost = (
        base_cost
        + volatility_cost
        + liquidity_cost
        + imbalance_cost
    )

    return round(total_cost, 4)


async def websocket_engine():
    stream_names = [
        f"{symbol.lower()}@bookTicker"
        for symbol in SYMBOLS
    ]

    stream_url = "/".join(stream_names)
    url = f"wss://stream.binance.com:9443/stream?streams={stream_url}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_queue=100
            ) as websocket:

                print("WebSocket connected")

                with data_lock:
                    bot_data["status"] = "CONNECTED"

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    stream_data = data["data"]

                    symbol = stream_data["s"]

                    bid = float(stream_data["b"])
                    ask = float(stream_data["a"])
                    mid_price = (bid + ask) / 2

                    update_price_history(symbol, mid_price)

                    with data_lock:
                        market_data[symbol] = {
                            "bid": bid,
                            "ask": ask,
                            "bid_qty": float(stream_data["B"]),
                            "ask_qty": float(stream_data["A"]),
                            "updated_at": time.time()
                        }

        except Exception as e:
            print("WebSocket error:", e)
            print("Reconnecting in 5 seconds...")

            with data_lock:
                bot_data["status"] = "RECONNECTING"

            await asyncio.sleep(5)


def fetch_depth(symbol):
    try:
        url = (
            f"https://api.binance.com/api/v3/depth"
            f"?symbol={symbol}&limit={DEPTH_LEVELS}"
        )

        data = requests.get(url, timeout=3).json()

        return {
            "asks": [
                [float(p), float(q)]
                for p, q in data["asks"]
            ],
            "bids": [
                [float(p), float(q)]
                for p, q in data["bids"]
            ],
            "updated_at": time.time()
        }

    except Exception:
        return None


def depth_engine():
    while True:
        try:
            for symbol in SYMBOLS:
                depth = fetch_depth(symbol)

                if depth:
                    with depth_lock:
                        depth_cache[symbol] = depth

            time.sleep(DEPTH_LOOP_INTERVAL)

        except Exception as e:
            print("Depth engine error:", e)
            time.sleep(3)


def simulate_market_buy(asks, quote_amount):
    base_received = 0
    quote_left = quote_amount

    for price, qty in asks:
        cost = price * qty

        if quote_left >= cost:
            base_received += qty
            quote_left -= cost
        else:
            base_received += quote_left / price
            break

    return base_received


def simulate_market_sell(bids, base_amount):
    quote_received = 0
    base_left = base_amount

    for price, qty in bids:
        if base_left >= qty:
            quote_received += price * qty
            base_left -= qty
        else:
            quote_received += base_left * price
            break

    return quote_received


def calculate_depth_profit(pairs):
    pair1, pair2, pair3 = pairs

    with depth_lock:
        d1 = depth_cache.get(pair1)
        d2 = depth_cache.get(pair2)
        d3 = depth_cache.get(pair3)

    if not d1 or not d2 or not d3:
        return None

    effective_cost = FEE_RATE + SLIPPAGE_RATE

    try:
        coin1 = simulate_market_buy(
            d1["asks"],
            START_USDT
        ) * (1 - effective_cost)

        coin2 = simulate_market_buy(
            d2["asks"],
            coin1
        ) * (1 - effective_cost)

        final_usdt = simulate_market_sell(
            d3["bids"],
            coin2
        ) * (1 - effective_cost)

        return round(final_usdt - START_USDT, 4)

    except Exception:
        return None


def scanner_engine():
    global last_good_opportunities
    global history
    global scanner_cycle_ms

    while True:
        cycle_start = time.time()

        try:
            opportunities = []
            effective_cost = FEE_RATE + SLIPPAGE_RATE

            with data_lock:
                snapshot = dict(market_data)

            for triangle in TRIANGLES:
                pair1, pair2, pair3 = triangle["pairs"]

                if not all(pair in snapshot for pair in [pair1, pair2, pair3]):
                    continue

                try:
                    p1 = snapshot[pair1]
                    p2 = snapshot[pair2]
                    p3 = snapshot[pair3]

                    liquidity = min(
                        p1["ask_qty"],
                        p2["ask_qty"],
                        p3["bid_qty"]
                    )

                    if liquidity < MIN_LIQUIDITY:
                        continue

                    coin1 = (START_USDT / p1["ask"]) * (1 - effective_cost)
                    coin2 = (coin1 / p2["ask"]) * (1 - effective_cost)
                    final_usdt = (coin2 * p3["bid"]) * (1 - effective_cost)

                    fast_profit = round(final_usdt - START_USDT, 4)

                    pair_vols = [
                        calculate_volatility(pair1),
                        calculate_volatility(pair2),
                        calculate_volatility(pair3)
                    ]

                    avg_volatility = round(
                        statistics.mean(pair_vols),
                        4
                    )

                    pair_imbalances = [
                        calculate_orderbook_imbalance(pair1),
                        calculate_orderbook_imbalance(pair2),
                        calculate_orderbook_imbalance(pair3)
                    ]

                    avg_imbalance = round(
                        statistics.mean(pair_imbalances),
                        4
                    )

                    route = triangle["name"]

                    market_latency_ms = round(
                        (
                            time.time()
                            - max(
                                p1["updated_at"],
                                p2["updated_at"],
                                p3["updated_at"]
                            )
                        ) * 1000,
                        2
                    )

                    with depth_lock:
                        depth_times = [
                            depth_cache.get(pair1, {}).get(
                                "updated_at",
                                time.time()
                            ),
                            depth_cache.get(pair2, {}).get(
                                "updated_at",
                                time.time()
                            ),
                            depth_cache.get(pair3, {}).get(
                                "updated_at",
                                time.time()
                            )
                        ]

                    depth_latency_ms = round(
                        (
                            time.time()
                            - max(depth_times)
                        ) * 1000,
                        2
                    )

                    depth_profit = calculate_depth_profit(
                        triangle["pairs"]
                    )

                    execution_cost = calculate_execution_cost(
                        depth_profit,
                        round(liquidity, 4),
                        avg_volatility,
                        avg_imbalance
                    )

                    if isinstance(depth_profit, float):
                        spread_percent = round(
                            (depth_profit / START_USDT) * 100,
                            4
                        )

                        adjusted_profit = round(
                            depth_profit - execution_cost,
                            4
                        )

                    else:
                        spread_percent = 0
                        adjusted_profit = 0

                    update_route_stats(
                        route,
                        depth_profit,
                        round(liquidity, 4),
                        avg_volatility
                    )

                    reliability_score = calculate_reliability_score(route)

                    is_profitable = (
                        isinstance(adjusted_profit, float)
                        and adjusted_profit > 0
                    )

                    opportunity = {
                        "route": route,
                        "pairs": triangle["pairs"],
                        "fast_profit": fast_profit,
                        "depth_profit": (
                            depth_profit
                            if depth_profit is not None
                            else "loading"
                        ),
                        "liquidity": round(liquidity, 4),
                        "avg_volatility": avg_volatility,
                        "avg_imbalance": avg_imbalance,
                        "reliability_score": reliability_score,
                        "spread_percent": spread_percent
                    }

                    opportunities.append(opportunity)

                    log = {
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "route": route,
                        "fast_profit": fast_profit,
                        "depth_profit": opportunity["depth_profit"],
                        "liquidity": round(liquidity, 4),
                        "avg_volatility": avg_volatility,
                        "avg_imbalance": avg_imbalance,
                        "reliability_score": reliability_score,
                        "spread_percent": spread_percent,
                        "market_latency_ms": market_latency_ms,
                        "depth_latency_ms": depth_latency_ms,
                        "scanner_cycle_ms": scanner_cycle_ms,
                        "execution_cost": execution_cost,
                        "adjusted_profit": adjusted_profit,
                        "is_profitable": is_profitable
                    }

                    history.append(log)

                    with open(CSV_FILE, "a", newline="") as file:
                        writer = csv.writer(file)
                        writer.writerow([
                            log["time"],
                            log["route"],
                            log["fast_profit"],
                            log["depth_profit"],
                            log["liquidity"],
                            log["avg_volatility"],
                            log["avg_imbalance"],
                            log["reliability_score"],
                            log["spread_percent"],
                            log["market_latency_ms"],
                            log["depth_latency_ms"],
                            log["scanner_cycle_ms"],
                            log["execution_cost"],
                            log["adjusted_profit"],
                            log["is_profitable"]
                        ])

                except Exception:
                    continue

            opportunities.sort(
                key=lambda x: x["fast_profit"],
                reverse=True
            )

            top_opportunities = opportunities[:5]

            scanner_cycle_ms = round(
                (time.time() - cycle_start) * 1000,
                2
            )

            history = history[-30:]

            if top_opportunities:
                last_good_opportunities = top_opportunities

            with data_lock:
                bot_data["top_opportunities"] = (
                    top_opportunities
                    if top_opportunities
                    else last_good_opportunities
                )

                bot_data["history"] = history[::-1]

                bot_data["last_update"] = (
                    datetime.now().strftime("%H:%M:%S")
                )

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            print("Scanner error:", e)
            time.sleep(3)


@app.route("/")
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Optimized Quant Arbitrage Dashboard</title>

        <style>
            body{
                background:#0f172a;
                color:white;
                font-family:Arial;
                padding:40px;
            }

            .card{
                background:#1e293b;
                padding:20px;
                border-radius:15px;
                margin-bottom:30px;
            }

            h1{
                color:#38bdf8;
            }

            table{
                width:100%;
                border-collapse:collapse;
                table-layout:fixed;
            }

            th,td{
                padding:12px;
                border-bottom:1px solid #334155;
                text-align:center;
                word-wrap:break-word;
            }

            th{
                background:#334155;
            }

            .profit{
                color:#22c55e;
                font-weight:bold;
            }

            .loss{
                color:#ef4444;
                font-weight:bold;
            }

            .yellow{
                color:#facc15;
                font-weight:bold;
            }

            .box{
                background:#0f172a;
                padding:15px;
                border-radius:10px;
                margin-bottom:15px;
            }
        </style>
    </head>

    <body>
        <h1>Optimized Quant Arbitrage Dashboard</h1>

        <div class="card">
            <h2>Status</h2>

            <p>
                Connection:
                <span id="status">Loading...</span>
            </p>

            <p>
                Last Update:
                <span id="last_update">Loading...</span>
            </p>
        </div>

        <div class="card">
            <h2>Live Opportunities</h2>

            <table>
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Route</th>
                        <th>Fast Profit</th>
                        <th>Depth Profit</th>
                        <th>Liquidity</th>
                        <th>Avg Volatility %</th>
                        <th>Avg Imbalance</th>
                        <th>Reliability</th>
                        <th>Spread %</th>
                    </tr>
                </thead>

                <tbody id="opportunities">
                    <tr>
                        <td colspan="9">Loading...</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>Recent Logs</h2>
            <div id="history">Loading...</div>
        </div>

        <script>
            async function loadData(){
                const response = await fetch('/data')
                const data = await response.json()

                document.getElementById("status").innerText =
                    data.status

                document.getElementById("last_update").innerText =
                    data.last_update

                let html = ""

                for(let [index, opp] of data.top_opportunities.entries()){

                    let fastClass =
                        opp.fast_profit > 0 ? "profit" : "loss"

                    let depthClass =
                        opp.depth_profit > 0 ? "profit" : "loss"

                    html += `
                    <tr>
                        <td>${index + 1}</td>

                        <td>${opp.route}</td>

                        <td class="${fastClass}">
                            $${opp.fast_profit}
                        </td>

                        <td class="${depthClass}">
                            $${opp.depth_profit}
                        </td>

                        <td class="yellow">
                            ${opp.liquidity}
                        </td>

                        <td>
                            ${opp.avg_volatility}
                        </td>

                        <td>
                            ${opp.avg_imbalance}
                        </td>

                        <td>
                            ${opp.reliability_score}
                        </td>

                        <td>
                            ${opp.spread_percent}%
                        </td>
                    </tr>
                    `
                }

                document.getElementById("opportunities").innerHTML = html

                let historyHtml = ""

                for(let log of data.history){
                    historyHtml += `
                    <div class="box">
                        <p><b>${log.time}</b></p>
                        <p>${log.route}</p>
                        <p>Fast Profit: $${log.fast_profit}</p>
                        <p>Depth Profit: $${log.depth_profit}</p>
                        <p>Liquidity: ${log.liquidity}</p>
                        <p>Avg Volatility: ${log.avg_volatility}</p>
                        <p>Avg Imbalance: ${log.avg_imbalance}</p>
                        <p>Reliability: ${log.reliability_score}</p>
                        <p>Spread %: ${log.spread_percent}%</p>
                        <p>Profitable: ${log.is_profitable}</p>
                    </div>
                    `
                }

                document.getElementById("history").innerHTML = historyHtml
            }

            setInterval(loadData, 1000)
        </script>
    </body>
    </html>
    """


@app.route("/data")
def data():
    with data_lock:
        return jsonify(bot_data)


def start_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_engine())


if __name__ == "__main__":

    websocket_thread = threading.Thread(
        target=start_websocket,
        daemon=True
    )

    depth_thread = threading.Thread(
        target=depth_engine,
        daemon=True
    )

    scanner_thread = threading.Thread(
        target=scanner_engine,
        daemon=True
    )

    websocket_thread.start()
    depth_thread.start()
    scanner_thread.start()

    port = int(os.environ.get("PORT", 5000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )