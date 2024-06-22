import asyncio
import json
import sqlite3
import websockets
from datetime import datetime, timezone
import pandas_ta as ta
import pandas as pd
import numpy as np
import constants

from order_flow_tools import calculate_order_flow_metrics

from get_signals import (get_spikes, generate_final_signal, market_sentiment_eval,
                         fetch_last_10_signals, fetch_last_4_hours_signals)

from kraken_toolbox import (get_open_positions, place_order, fetch_candles_since,
                            fetch_live_price, fetch_last_n_candles, KrakenFuturesAuth)


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


def insert_position(symbol, open_price, side, size, take_profit):
    timestamp = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    INSERT INTO opened_positions (symbol, timestamp, open_price, side, size, take_profit)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (symbol, timestamp, open_price, side, size, take_profit))
    conn.commit()


def close_position(position_id, close_price):
    close_time = int(datetime.now(timezone.utc).timestamp())
    cursor.execute("""
    UPDATE opened_positions
    SET close_price = ?, close_time = ?
    WHERE id = ?
    """, (close_price, close_time, position_id))
    conn.commit()


def fetch_open_position(symbol):
    cursor.execute("""
    SELECT *
    FROM opened_positions
    WHERE close_price IS NULL AND symbol = ?
    """, (symbol,))
    open_positions = cursor.fetchall()
    return open_positions


# Function to check if the two most recent signals are 'buy' or 'sell'
def check_for_consecutive_signals(signals):
    global stored_signal

    if len(signals) < 3:
        return None

    first_signal = signals[0][0]
    second_signal = signals[1][0]
    third_signal = signals[2][0]

    if first_signal == 'buy' and second_signal == 'buy' and third_signal == 'buy':
        stored_signal = 'buy'
        return 'buy'

    elif first_signal == 'sell' and second_signal == 'sell' and third_signal == 'sell':
        stored_signal = 'sell'
        return 'sell'

    elif first_signal == 'hold' and second_signal == 'hold' and third_signal == 'hold':
        stored_signal = 'hold'
        return 'hold'

    return None


def calculate_atr(df, period=14):
    df['previous_close'] = df['close'].shift(1)
    df['H-L'] = df['high'] - df['low']
    df['H-PC'] = (df['high'] - df['previous_close']).abs()
    df['L-PC'] = (df['low'] - df['previous_close']).abs()
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    atr = df['TR'].rolling(window=period).mean().iloc[-1]
    return atr


def calculate_stochastic_rsi(df):
    df = ta.stochrsi(df['close'], length=14, rsi_length=14, k=3, d=3)
    # print(df)
    return df


def check_stochastic_setup(df):

    print(df.iloc[-1])

    # Check for buy setup (if %K > %D and %K < 20)
    if df['STOCHRSId_14_14_3_3'].iloc[-1] < df['STOCHRSIk_14_14_3_3'].iloc[-1] < 20:
        return 'buy'

    # Check for sell setup (if %D > %K and %K > 80)
    elif df['STOCHRSId_14_14_3_3'].iloc[-1] > df['STOCHRSIk_14_14_3_3'].iloc[-1] > 80:
        return 'sell'

    else:
        return None


def calculate_rsi(df, period=14):
    df['rsi'] = ta.rsi(df['close'], length=period)
    return df['rsi'].iloc[-1]


def get_rsi(symbol, period=14):
    data = fetch_last_n_candles(symbol, num_candles=period+1)  # Fetch the required historical data
    rsi_value = calculate_rsi(data, period)
    return rsi_value


def calculate_average_move(symbol):
    # Fetch the 4-hour data for the symbol
    df = fetch_last_n_candles(symbol, interval=60, num_candles=60)

    # Ensure the data is in the correct format
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    # Calculate the move for each 4-hour candle
    df['move'] = df['high'] - df['low']

    # Calculate the average move
    average_move = df['move'].mean()
    print(average_move)

    return average_move


def get_take_profit(symbol, side, current_price):
    average_move = calculate_average_move(symbol)
    take_profit = None

    if side == 'buy':
        take_profit = current_price + (average_move / 1)
    if side == 'sell':
        take_profit = current_price - (average_move / 1)

    return take_profit


def calculate_williams_fractals(df, period=7):
    # Ensure that the high and low columns are numeric and replace non-numeric with NaN
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')

    # Ensure all values are numeric (convert NaN to a very small number)
    df['high'] = df['high'].fillna(value=np.nan)
    df['low'] = df['low'].fillna(value=np.nan)

    # Calculate the Williams Fractals
    def fractal_up(series):
        center = len(series) // 2
        if series[center] == max(series):
            return series[center]
        return np.nan

    def fractal_down(series):
        center = len(series) // 2
        if series[center] == min(series):
            return series[center]
        return np.nan

    df['fractal_up'] = df['high'].rolling(window=2 * period + 1, center=True).apply(fractal_up, raw=True)
    df['fractal_down'] = df['low'].rolling(window=2 * period + 1, center=True).apply(fractal_down, raw=True)

    return df


def manage_positions(symbol, size):
    global stored_signal

    # Fetch recent signals and check for consecutive signals
    # last_10_signals = fetch_last_10_signals()
    # check_for_consecutive_signals(last_10_signals)

    # Market sentiment
    sentiment_signals = fetch_last_4_hours_signals()
    market_sentiment = market_sentiment_eval(sentiment_signals)[1]

    # Check stochastic setup
    df = fetch_last_n_candles('XXBTZUSD', num_candles=60)
    rsi_df = calculate_stochastic_rsi(df)
    stoch_setup = check_stochastic_setup(rsi_df)

    # Fetch positions and current price
    open_positions = get_open_positions(open_pos_auth)
    current_price = fetch_live_price(symbol)['last_price']

    # Fetch open position from the database
    db_positions = fetch_open_position(symbol)
    rsi_value = get_rsi('XXBTZUSD')

    print('Open positions from DB:', db_positions)
    # print('RSI:', rsi_value)
    calculate_average_move('XXBTZUSD')

    # Extract position details if there are open positions in the database
    if db_positions:
        position_id, pos_symbol, open_timestamp, open_price, side, size, tp, sl, close_price, close_time = db_positions[-1]

    # Check for open positions via API
    if open_positions and 'openPositions' in open_positions and open_positions['openPositions']:
        print('Open positions from API:', open_positions['openPositions'])
        for position in open_positions['openPositions']:
            if position['symbol'] == symbol and position['side'] == 'short':
                print('Evaluating short position for symbol:', symbol)
                if stoch_setup == 'buy' or current_price <= tp:
                    print('Closing short position and opening long position.')
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, current_price)
            elif position['symbol'] == symbol and position['side'] == 'long':
                print('Evaluating long position for symbol:', symbol)
                if stoch_setup == 'sell' or current_price >= tp:
                    print('Closing long position and opening short position.')
                    place_order(order_auth, symbol, 'sell', position['size'])
                    close_position(position_id, current_price)

    # Conditions to OPEN positions
    if not open_positions['openPositions']:
        print('No open positions found.')
        if market_sentiment == 'buy' and stoch_setup == 'buy':
            print('Placing new buy order.')
            place_order(order_auth, symbol, 'buy', size)
            take_profit = get_take_profit('XXBTZUSD', 'buy', current_price)
            insert_position(symbol, current_price, 'long', size, take_profit)
        elif stored_signal == 'sell' and stoch_setup == 'sell':
            print('Placing new sell order.')
            place_order(order_auth, symbol, 'sell', size)
            take_profit = get_take_profit('XXBTZUSD', 'sell', current_price)
            insert_position(symbol, current_price, 'short', size, take_profit)


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


# Function to check for trailing stop using fractals
def check_trailing_stop(symbol):
    db_positions = fetch_open_position(symbol)
    if db_positions:
        for position in db_positions:
            position_id, open_price, side, size, open_timestamp = position
            current_price = fetch_live_price(symbol)['last_price']

            # Fetch historical data since the position was opened
            historical_data = fetch_last_n_candles('XXBTZUSD', interval=5, num_candles=60)
            fractals_data = calculate_williams_fractals(historical_data)

            if side == 'long':
                recent_down_fractals = fractals_data['fractal_down'].dropna()
                if not recent_down_fractals.empty:
                    trailing_stop_price = recent_down_fractals.iloc[-1]
                    if current_price < trailing_stop_price:
                        place_order(order_auth, symbol, 'sell', size)
                        close_position(position_id, current_price)

            elif side == 'short':
                recent_up_fractals = fractals_data['fractal_up'].dropna()
                if not recent_up_fractals.empty:
                    trailing_stop_price = recent_up_fractals.iloc[-1]
                    if current_price > trailing_stop_price:
                        place_order(order_auth, symbol, 'buy', size)
                        close_position(position_id, current_price)


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
    final_signal = generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold=9)

    # Market sentiment
    sentiment_signals = fetch_last_4_hours_signals()
    market_pressure = market_sentiment_eval(sentiment_signals)[0]

    # Assuming 'volume_profile_signal' and 'price_action_signal' are obtained from other analyses
    volume_profile_signal = "N/A"  # Placeholder
    price_action_signal = "N/A"  # Placeholder

    # Insert the signal into the database
    timestamp = int(datetime.now(timezone.utc).timestamp())
    insert_signal(timestamp, final_signal[0], final_signal[1], market_pressure, volume_profile_signal, price_action_signal)

    # Manage positions based on the signals
    manage_positions('PF_XBTUSD', 0.002)
    # check_trailing_stop('PF_XBTUSD')


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
