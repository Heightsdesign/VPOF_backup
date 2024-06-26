import asyncio
import json
import sqlite3
import websockets
from datetime import datetime, timezone, timedelta
import pandas_ta as ta
import pandas as pd
import numpy as np
import constants
from sklearn.linear_model import LinearRegression


from order_flow_tools import calculate_order_flow_metrics

from get_signals import (get_spikes, generate_final_signal, market_sentiment_eval,
                         fetch_last_10_signals, fetch_last_n_hours_signals, fetch_last_24_hours_signals)

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


def fetch_trades(minutes=960):
    conn = sqlite3.connect('trading_data.db')
    cursor = conn.cursor()

    # Calculate the timestamp for the starting point
    current_time = datetime.now()
    start_time = current_time - timedelta(minutes=minutes)
    start_timestamp = int(start_time.timestamp())

    # Fetch trades from the database
    cursor.execute("""
    SELECT timestamp, price, volume, side, type_order
    FROM trades
    WHERE timestamp >= ?
    ORDER BY timestamp ASC
    """, (start_timestamp,))

    trades = cursor.fetchall()

    # Convert to DataFrame
    trade_data = pd.DataFrame(trades, columns=['timestamp', 'price', 'volume', 'side', 'type_order'])

    # Convert timestamp to datetime
    trade_data['timestamp'] = pd.to_datetime(trade_data['timestamp'], unit='s')

    conn.close()
    return trade_data


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


# Function to check if the two most recent signals are 'buy' or 'sell'
def check_short_term_activity(signals):
    global stored_signal

    if len(signals) < 3:
        return None

    buys = 0
    sells = 0

    for signal in signals:
        if signal[0] == 'buy':
            buys += 1
            sells = 0
        elif signal[0] == 'sell':
            sells += 1
            buys = 0

    if buys >= 3:
        return 'buy'
    elif sells >= 3:
        return 'sell'
    else:
        return 'hold'


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
    if df['STOCHRSId_14_14_3_3'].iloc[-1] < df['STOCHRSIk_14_14_3_3'].iloc[-1]:
        return 'buy'

    # Check for sell setup (if %D > %K and %K > 80)
    elif df['STOCHRSId_14_14_3_3'].iloc[-1] > df['STOCHRSIk_14_14_3_3'].iloc[-1]:
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


def create_dollar_bars(trade_data, dollar_threshold):
    dollar_bars = []
    temp_dollar = 0
    open_price = trade_data['price'].iloc[0]
    high_price = trade_data['price'].iloc[0]
    low_price = trade_data['price'].iloc[0]
    close_price = trade_data['price'].iloc[0]

    for index, row in trade_data.iterrows():
        trade_dollar = row['price'] * row['volume']
        temp_dollar += trade_dollar
        high_price = max(high_price, row['price'])
        low_price = min(low_price, row['price'])
        close_price = row['price']

        if temp_dollar >= dollar_threshold:
            dollar_bars.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'dollar_volume': temp_dollar,
                'timestamp': row['timestamp']
            })
            temp_dollar = 0
            open_price = row['price']
            high_price = row['price']
            low_price = row['price']

    return pd.DataFrame(dollar_bars)


def calculate_average_move(symbol):
    dollar_bars_trade_data = fetch_trades()
    dollar_bars = create_dollar_bars(dollar_bars_trade_data, 5000000)
    average_move = (dollar_bars['high'] - dollar_bars['low']).mean()
    return average_move / 2


def get_stops(symbol, side, current_price):
    average_move = calculate_average_move(symbol)
    take_profit = None
    stop_loss = None

    if side == 'buy':
        take_profit = current_price + average_move
        stop_loss = current_price - average_move
    if side == 'sell':
        take_profit = current_price - average_move
        stop_loss = current_price + average_move

    return take_profit, stop_loss


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


def calculate_slope_pressure(pressure_data):

    x = np.arange(len(pressure_data)).reshape(-1, 1)
    y = np.array(pressure_data).reshape(-1, 1)
    model = LinearRegression().fit(x, y)
    slope = model.coef_[0][0]

    return slope


def define_thresholds(pressure_data):

    mean_pressure = np.mean(pressure_data)
    std_pressure = np.std(pressure_data)
    upper_threshold = mean_pressure + 1.5 * std_pressure
    lower_threshold = mean_pressure - 1.5 * std_pressure

    return upper_threshold, lower_threshold


def calculate_dollar_volume_since_open(position_open_time):

    cursor.execute("""
    SELECT SUM(price * volume) 
    FROM trades 
    WHERE timestamp >= ?
    """, (position_open_time,))
    dollar_volume = cursor.fetchone()[0]
    return dollar_volume if dollar_volume is not None else 0


def manage_positions(symbol, size):
    global stored_signal

    # Fetch recent signals and check for consecutive signals
    last_10_signals = fetch_last_10_signals()
    short_term_activity = check_short_term_activity(last_10_signals)

    # Market sentiment
    sentiment_signals = fetch_last_n_hours_signals(1)
    market_sentiment = market_sentiment_eval(sentiment_signals)[1]

    # Daily signals
    daily_signals = fetch_last_24_hours_signals()

    # Check stochastic setup
    df = fetch_last_n_candles('XXBTZUSD', num_candles=60)
    rsi_df = calculate_stochastic_rsi(df)
    stoch_setup = check_stochastic_setup(rsi_df)

    # Fetch positions and current price
    open_positions = get_open_positions(open_pos_auth)
    current_price = fetch_live_price(symbol)['last_price']

    # Fetch open position from the database
    db_positions = fetch_open_position(symbol)
    # rsi_value = get_rsi('XXBTZUSD')

    short_term_pressure = [signal[2] for signal in last_10_signals]
    long_term_pressure = [signal[4] for signal in daily_signals]

    dollar_volume_since_open = None

    slope = calculate_slope_pressure(short_term_pressure)
    upper_threshold, lower_threshold = define_thresholds(long_term_pressure)
    print('Pressure Slope : ', slope)
    print('Open positions from DB:', db_positions)
    # print('RSI:', rsi_value)
    print('Stochastic Setup : ', stoch_setup)
    print('Short Term Activity : ', short_term_activity)
    calculate_average_move('XXBTZUSD')

    dollar_bars_trade_data = fetch_trades()
    print(create_dollar_bars(dollar_bars_trade_data, 5000000))

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

                elif dollar_volume_since_open >= 5000000:  # Threshold for the dollar-volume-based exit
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'dollar_volume_exit', current_price)

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

                elif dollar_volume_since_open >= 5000000:  # Threshold for the dollar-volume-based exit
                    place_order(order_auth, symbol, 'buy', position['size'])
                    close_position(position_id, 'dollar_volume_exit', current_price)

    # Conditions to OPEN positions
    if not open_positions['openPositions']:
        print('No open positions found.')

        if stoch_setup == 'buy' and slope < 0:
            print('Placing new buy order.')
            place_order(order_auth, symbol, 'buy', size)
            take_profit, stop_loss = get_stops('XXBTZUSD', 'buy', current_price)
            insert_position(symbol, current_price, 'long', size, take_profit, stop_loss)

        elif stoch_setup == 'sell' and slope > 0:
            print('Placing new sell order.')
            place_order(order_auth, symbol, 'sell', size)
            take_profit, stop_loss = get_stops('XXBTZUSD', 'sell', current_price)
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
    sentiment_signals = fetch_last_n_hours_signals(1)
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
