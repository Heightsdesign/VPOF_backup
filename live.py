import asyncio
import json
import sqlite3
import websockets
from datetime import datetime, timezone
import numpy as np
import constants
from sklearn.linear_model import LinearRegression

from constants import dollar_threshold
from dollar_bars import fetch_trades, create_dollar_bars
from get_signals import get_market_signal, calculate_stochastic_rsi, check_stochastic_setup
from kraken_toolbox import (get_open_positions, place_order,
                            fetch_live_price, KrakenFuturesAuth)


# Database connection
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()

# Store channel ID for trades
trade_channel_id = None
order_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/sendorder')
open_orders_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openorders')
open_pos_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openpositions')
stored_signal = None
position_ids = {}


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


def insert_signal(
        timestamp, order_flow_signal, order_flow_score, market_pressure, volume_profile_signal, price_action_signal):
    cursor.execute("""
    INSERT INTO signals (timestamp, order_flow_signal, order_flow_score, market_pressure, volume_profile_signal, price_action_signal)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (timestamp, order_flow_signal, order_flow_score, market_pressure, volume_profile_signal, price_action_signal))
    conn.commit()


def insert_position(symbol, open_price, side, size, take_profit, stop_loss):
    timestamp = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    INSERT INTO opened_positions (symbol, timestamp, open_price, side, size, take_profit, stop_loss)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (symbol, timestamp, open_price, side, size, take_profit, stop_loss))
    conn.commit()


def close_position(position_id, close_reason, close_price):
    close_time = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    UPDATE opened_positions
    SET close_reason = ?, close_price = ?, close_time = ?
    WHERE id = ?
    """, (close_reason, close_price, close_time, position_id))
    conn.commit()


def fetch_open_position(symbol):
    cursor.execute("""
    SELECT *
    FROM opened_positions
    WHERE close_price IS NULL AND symbol = ?
    """, (symbol,))
    open_positions = cursor.fetchall()
    return open_positions


def calculate_average_move(dollar_bars, num_bars):
    # Check if dollar_bars DataFrame is empty or if it contains fewer bars than num_bars
    if dollar_bars.empty or len(dollar_bars) < num_bars:
        print("Not enough dollar bars available.")
        return None

    selected_bars = dollar_bars.tail(num_bars)  # Select the last num_bars rows
    average_move = (selected_bars['high'] - selected_bars['low']).mean()
    return average_move * 2


def get_stops(dollar_bars, side, current_price):

    take_profit = None
    stop_loss = None
    average_move = calculate_average_move(dollar_bars, 7)

    if side == 'buy':
        take_profit = current_price + average_move
        stop_loss = current_price - average_move
    if side == 'sell':
        take_profit = current_price - average_move
        stop_loss = current_price + average_move

    return take_profit, stop_loss


def calculate_slope_pressure(pressure_data):

    x = np.arange(len(pressure_data)).reshape(-1, 1)
    y = np.array(pressure_data).reshape(-1, 1)
    model = LinearRegression().fit(x, y)
    slope = model.coef_[0][0]

    return slope


def calculate_dollar_volume_since_open(position_open_time):

    cursor.execute("""
    SELECT SUM(price * volume) 
    FROM trades 
    WHERE timestamp >= ?
    """, (position_open_time,))
    dollar_volume = cursor.fetchone()[0]
    return dollar_volume if dollar_volume is not None else 0


def manage_positions(symbol, size, dollar_bars, num_bars):
    # Fetch positions and current price
    open_positions = get_open_positions(open_pos_auth)
    current_price = fetch_live_price(symbol)['last_price']
    db_positions = fetch_open_position(symbol)

    print('Open positions from DB:', db_positions)

    # Get signal
    signal = get_market_signal(dollar_bars, num_bars, 3)
    stoch_rsi = calculate_stochastic_rsi(dollar_bars)
    setup = check_stochastic_setup(stoch_rsi)

    print(f"Market Signal: {signal}")

    # Extract position details if there are open positions in the database
    if db_positions:
        (position_id, pos_symbol, open_timestamp, open_price,
         side, size, tp, sl, close_reason, close_price, close_time) = db_positions[-1]

        # Calculate the dollar volume since the position was opened
        dollar_volume_since_open = calculate_dollar_volume_since_open(open_timestamp)

    # Check for open positions via API
    if open_positions and 'openPositions' in open_positions and open_positions['openPositions']:
        print('Open positions from API:', open_positions['openPositions'])
        for position in open_positions['openPositions']:
            if position['symbol'] == symbol and position['side'] == 'short':
                print('Evaluating short position for symbol:', symbol)

                if current_price <= tp:
                    print('Closing short position due to take profit.')
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'take_profit', current_price)

                elif current_price >= sl:
                    print('Closing short position due to stop loss.')
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'stop_loss', current_price)

                elif dollar_volume_since_open >= dollar_threshold * num_bars:
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'dollar_volume_exit', current_price)

                elif signal == 'buy':
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'market_switch_exit', current_price)

            elif position['symbol'] == symbol and position['side'] == 'long':
                print('Evaluating long position for symbol:', symbol)

                if current_price >= tp:
                    print('Closing long position due to take profit')
                    place_order(order_auth, symbol, 'sell', position['size'])
                    close_position(position_id, 'take_profit', current_price)

                elif current_price <= sl:
                    print('Closing long position due to stop loss')
                    place_order(order_auth, symbol, 'sell', position['size'])
                    close_position(position_id, 'stop_loss', current_price)

                elif dollar_volume_since_open >= dollar_threshold * num_bars:
                    place_order(order_auth, symbol, 'sell', position['size'])
                    close_position(position_id, 'dollar_volume_exit', current_price)

                elif signal == 'sell':
                    place_order(order_auth, symbol, 'sell', position['size'])
                    close_position(position_id, 'market_switch_exit', current_price)

    # Conditions to OPEN positions
    if not open_positions['openPositions']:
        print('No open positions found.')

        if signal == 'buy' and setup == 'buy':
            print('Placing new buy order.')
            place_order(order_auth, symbol, 'buy', size)
            take_profit, stop_loss = get_stops(dollar_bars, 'buy', current_price)
            insert_position(symbol, current_price, 'long', size, take_profit, stop_loss)

        elif signal == 'sell' and setup == 'sell':
            print('Placing new sell order.')
            place_order(order_auth, symbol, 'sell', size)
            take_profit, stop_loss = get_stops(dollar_bars, 'sell', current_price)
            insert_position(symbol, current_price, 'short', size, take_profit, stop_loss)


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
            # print("Received data:", data)  # Enhanced logging

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
                    # print(f"Inserted trade data")


def run_analysis_and_store_signals():

    # Fetch trades and create dollar bars
    trade_data = fetch_trades(hours=48)
    dollar_bars = create_dollar_bars(trade_data, threshold=constants.dollar_threshold)

    if dollar_bars.empty:
        print("No dollar bars available for analysis.")
        return

    print("Dollar bars created successfully")

    # Manage positions based on the signals
    manage_positions('PF_XBTUSD', 0.002, dollar_bars, 7)


# Periodically run the analysis and store signals
async def periodic_analysis(interval):
    while True:
        run_analysis_and_store_signals()
        await asyncio.sleep(interval)


# Main function to run WebSocket and analysis concurrently
async def main():
    websocket_task = asyncio.create_task(kraken_websocket())
    analysis_task = asyncio.create_task(periodic_analysis(300))  # Run analysis every 5 minutes
    await asyncio.gather(websocket_task, analysis_task)


asyncio.run(main())
