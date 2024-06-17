import asyncio
import json
import sqlite3
import websockets
from datetime import datetime, timezone
import constants

from order_flow_tools import calculate_order_flow_metrics
from get_signals import get_spikes, generate_final_signal, fetch_last_10_signals
from kraken_toolbox import get_open_positions, place_order, fetch_live_price, KrakenFuturesAuth


# Database connection
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()

# Store channel ID for trades
trade_channel_id = None
order_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/sendorder')
open_orders_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openorders')
open_pos_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openpositions')


# Function to insert trade data
def insert_trade(trades):
    for trade in trades:
        print(f"Processing trade: {trade}")  # Log each trade
        price, volume, trade_time, side, type_order, *_ = trade
        side = 'buy' if side == 'b' else 'sell'
        type_order = 'market' if 'm' in trade[4:] else 'limit'
        cursor.execute("INSERT INTO trades (timestamp, price, volume, side, type_order) VALUES (?, ?, ?, ?, ?)",
                       (trade_time, price, volume, side, type_order))
    conn.commit()


def insert_signal(timestamp, order_flow_signal, volume_profile_signal, price_action_signal):
    cursor.execute("""
    INSERT INTO signals (timestamp, order_flow_signal, volume_profile_signal, price_action_signal)
    VALUES (?, ?, ?, ?)
    """, (timestamp, order_flow_signal, volume_profile_signal, price_action_signal))
    conn.commit()


# Function to check for three consecutive buy or sell signals
def check_for_consecutive_signals(signals):
    buy_count = 0
    sell_count = 0

    for signal in signals:
        if signal[0] == 'buy':
            buy_count += 1
            sell_count -= 1
        elif signal[0] == 'sell':
            sell_count += 1
            buy_count -= 1

        if buy_count >= 3:
            return 'buy'
        if sell_count >= 3:
            return 'sell'

    return None


def insert_opened_position(open_price, side, size):
    timestamp = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    INSERT INTO opened_positions (timestamp, open_price, side, size)
    VALUES (?, ?, ?, ?)
    """, (timestamp, open_price, side, size))
    conn.commit()


def close_position(position_id, close_price):
    close_time = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    UPDATE opened_positions
    SET close_price = ?, close_time = ?
    WHERE id = ?
    """, (close_price, close_time, position_id))
    conn.commit()


def manage_positions(symbol, size):
    last_10_signals = fetch_last_10_signals()
    signal = check_for_consecutive_signals(last_10_signals)
    open_positions = get_open_positions(open_pos_auth)
    print(open_positions)

    if open_positions and 'openPositions' in open_positions and open_positions['openPositions']:
        if signal == 'buy':
            for position in open_positions['openPositions']:
                if position['symbol'] == symbol and position['side'] == 'short':
                    # Close any sell positions and open buy position
                    place_order(order_auth, symbol, 'buy', position['size'] * 2)
                    close_position(position['id'], position['current_price'])
                    insert_opened_position(position['current_price'], 'long', position['size'] * 2)
                elif position['symbol'] == symbol and position['side'] == 'long':
                    break

        elif signal == 'sell':
            for position in open_positions['openPositions']:
                if position['symbol'] == symbol and position['side'] == 'long':
                    # Close any buy positions and open sell position
                    place_order(order_auth, symbol, 'sell', position['size'] * 2)
                    close_position(position['id'], position['current_price'])
                    insert_opened_position(position['current_price'], 'short', position['size'] * 2)
                elif position['symbol'] == symbol and position['side'] == 'short':
                    break
    else:
        current_price = fetch_live_price(symbol)['last_price']
        if signal == 'buy':
            place_order(order_auth, symbol, 'buy', size)
            insert_opened_position(current_price, 'long', size)
        elif signal == 'sell':
            place_order(order_auth, symbol, 'sell', size)
            insert_opened_position(current_price, 'short', size)


def check_trailing_stop(symbol):
    cursor.execute("""
    SELECT id, open_price, side, size
    FROM opened_positions
    WHERE close_price IS NULL
    """)
    open_positions = cursor.fetchall()

    for position in open_positions:
        position_id, open_price, side, size = position
        current_price = fetch_live_price(symbol)['last_price']
        if side == 'long':
            highest_price = max(get_highest_price_since_open(position_id), current_price)
            if (highest_price - open_price) > 0:
                if (highest_price - current_price) / (highest_price - open_price) > 0.5:
                    place_order(order_auth, symbol, 'sell', size)
                    close_position(position_id, current_price)
        elif side == 'short':
            lowest_price = min(get_lowest_price_since_open(position_id), current_price)
            if (open_price - lowest_price) > 0:
                if (current_price - lowest_price) / (open_price - lowest_price) > 0.5:
                    place_order(order_auth, symbol, 'buy', size)
                    close_position(position_id, current_price)


def get_highest_price_since_open(position_id):
    cursor.execute("""
    SELECT MAX(price)
    FROM trades
    WHERE timestamp >= (SELECT timestamp FROM opened_positions WHERE id = ?)
    """, (position_id,))
    return cursor.fetchone()[0]


def get_lowest_price_since_open(position_id):
    cursor.execute("""
    SELECT MIN(price)
    FROM trades
    WHERE timestamp >= (SELECT timestamp FROM opened_positions WHERE id = ?)
    """, (position_id,))
    return cursor.fetchone()[0]


# WebSocket handler
async def kraken_websocket():
    global trade_channel_id
    uri = "wss://ws.kraken.com/"

    async with websockets.connect(uri) as websocket:
        # Subscribe to the BTC/USD trade feed
        await websocket.send(json.dumps({
            "event": "subscribe",
            "pair": ["XBT/USD"],
            "subscription": {"name": "trade"}
        }))

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            print("Received data:", data)  # Enhanced logging

            # Handle subscription status messages
            if isinstance(data, dict) and data.get("event") == "subscriptionStatus":
                print("Subscription status:", data)
                if data["subscription"]["name"] == "trade":
                    trade_channel_id = data["channelID"]
                continue

            # Differentiate trade data based on channel ID
            if isinstance(data, list) and len(data) > 1:
                channel_id = data[0]
                if channel_id == trade_channel_id:
                    trades = data[1]
                    insert_trade(trades)
                    print(f"Inserted trade data")


# Function to run the order flow analysis and store signals
def run_analysis_and_store_signals():
    # Your analysis logic
    (delta_values, cumulative_delta, min_delta_values,
     max_delta_values, market_buy_ratios, market_sell_ratios,
     buy_volumes, sell_volumes, aggressive_buy_activities,
     aggressive_sell_activities, aggressive_ratios) = calculate_order_flow_metrics()

    aggressive_ratio_signals = get_spikes(aggressive_ratios)
    delta_value_signals = get_spikes(delta_values)

    # Calculate final signal
    final_signal = generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold=10)

    # Assuming 'volume_profile_signal' and 'price_action_signal' are obtained from other analyses
    volume_profile_signal = "N/A"  # Placeholder
    price_action_signal = "N/A"  # Placeholder

    # Insert the signal into the database
    timestamp = int(datetime.now(timezone.utc).timestamp())
    insert_signal(timestamp, final_signal, volume_profile_signal, price_action_signal)

    # Manage positions based on the signals
    manage_positions('PF_XBTUSD', 0.001)


# Periodically run the analysis and store signals
async def periodic_analysis(interval):
    while True:
        run_analysis_and_store_signals()
        check_trailing_stop('PF_XBTUSD')
        await asyncio.sleep(interval)


# Main function to run WebSocket and analysis concurrently
async def main():
    websocket_task = asyncio.create_task(kraken_websocket())
    analysis_task = asyncio.create_task(periodic_analysis(300))  # Run analysis every 5 minutes
    await asyncio.gather(websocket_task, analysis_task)


asyncio.run(main())
