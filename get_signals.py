import numpy as np
import sqlite3
from datetime import datetime, timezone, timedelta
import pandas_ta as ta

from order_flow_tools import calculate_order_flow_metrics
from dollar_bars import dollar_bars



# Ensure the database connection is open
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()


def calculate_stochastic_rsi(df):
    df = ta.stochrsi(df['close'], length=14, rsi_length=14, k=3, d=3)
    return df


def get_rsi(df, period=14):
    # Check if dollar_bars DataFrame is empty
    if df.empty:
        print("No dollar bars available.")
        return None

    # Calculate RSI
    df['RSI'] = ta.rsi(df['close'], length=period)

    # Return the last RSI value as an integer
    return int(df['RSI'].iloc[-1])


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


# Function to score signals and generate a final decision
def get_delta_rating(deltas, num_bars):
    # Ensure there are enough bars to analyze
    if len(deltas) < num_bars:
        return 'Not enough data', None

    cum_delta = 0

    for delta in deltas[-num_bars:]:
        cum_delta += delta

    if cum_delta > 0:
        rating = ["buy", cum_delta]
    elif cum_delta < 0:
        rating = ["sell", cum_delta]
    else:
        rating = ['hold', cum_delta]

    print('Deltas:', deltas[-num_bars:])
    print('Cumulative Delta:', cum_delta)

    return rating


def get_price_action_rating(dol_bars, num_bars):
    # Ensure there are enough bars to analyze
    if len(dol_bars) < num_bars:
        return 'Not enough data', None

    start_close = dol_bars['close'].iloc[-num_bars]
    end_close = dol_bars['close'].iloc[-1]

    if end_close > start_close:
        return 'buy'
    elif end_close < start_close:
        return 'sell'
    else:
        return 'neutral', 0


def get_market_signal(dollar_bars, num_bars, num_ratings):
    (delta_values, cumulative_delta, min_delta_values,
     max_delta_values, market_buy_ratios, market_sell_ratios,
     buy_volumes, sell_volumes, aggressive_buy_activities,
     aggressive_sell_activities, aggressive_ratios, latest_bar) = calculate_order_flow_metrics(dollar_bars)

    delta_ratings = []
    setup_score = 0
    total_cum_delta = 0

    for i in range(num_ratings):
        delta_rating = get_delta_rating(aggressive_ratios, num_bars * (i + 1))
        delta_ratings.append(delta_rating)

        if delta_rating[0] == 'buy':
            setup_score += 1
            total_cum_delta += delta_rating[1]

        elif delta_rating[0] == 'sell':
            setup_score -= 1
            total_cum_delta += delta_rating[1]

    long_term_rating = get_delta_rating(aggressive_ratios, 49)
    delta_ratings.append(long_term_rating)

    if long_term_rating[0] == 'buy':
        setup_score += 1
        total_cum_delta += long_term_rating[1]

    elif long_term_rating[0] == 'sell':
        setup_score -= 1
        total_cum_delta += long_term_rating[1]

    if total_cum_delta > 0:
        setup_score += 1
    else:
        setup_score -= 1

    print('Delta Ratings : ', delta_ratings)
    print('Total Cumulative Delta', total_cum_delta)

    if setup_score > len(delta_ratings) / 2:
        signal = 'buy'
    elif setup_score < (len(delta_ratings) / 2) * -1:
        signal = 'sell'
    else:
        signal = 'hold'

    print(f"Final Signal : {signal}")
    return signal


# Function to fetch the last 'buy' signal
def fetch_last_buy_signal():
    cursor.execute("""
    SELECT * FROM signals 
    WHERE order_flow_signal = 'buy' 
    ORDER BY timestamp DESC 
    LIMIT 1
    """)
    last_buy_signal = cursor.fetchone()
    if last_buy_signal:
        signal_id, timestamp, order_flow_signal, orderflow_score, market_pressure, volume_profile_signal, price_action_signal = last_buy_signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Order Flow Score: {orderflow_score}, Market Pressure: {market_pressure}")
    else:
        print("No 'buy' signals found.")

    return last_buy_signal


# Function to fetch the last 'sell' signal
def fetch_last_sell_signal():
    cursor.execute("""
    SELECT * FROM signals 
    WHERE order_flow_signal = 'sell' 
    ORDER BY timestamp DESC 
    LIMIT 1
    """)
    last_sell_signal = cursor.fetchone()
    if last_sell_signal:
        signal_id, timestamp, order_flow_signal, orderflow_score, market_pressure, volume_profile_signal, price_action_signal = last_sell_signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Order Flow Score: {orderflow_score} , Market Pressure: {market_pressure}")
    else:
        print("No 'sell' signals found.")

    return last_sell_signal


def fetch_last_n_hours_signals(hours):
    current_time = datetime.now(timezone.utc)
    start_timestamp = int((current_time - timedelta(hours=hours)).timestamp())

    cursor.execute("""
    SELECT order_flow_signal, order_flow_score FROM signals 
    WHERE timestamp >= ? 
    ORDER BY timestamp ASC
    """, (start_timestamp,))
    last_4_hours_signals = cursor.fetchall()

    return last_4_hours_signals


def fetch_last_10_signals():
    cursor.execute("""
    SELECT order_flow_signal, order_flow_score, market_pressure FROM signals 
    ORDER BY timestamp ASC
    LIMIT 10
    """)
    return cursor.fetchall()


def fetch_all_opened_positions():
    cursor.execute("""
    SELECT *
    FROM opened_positions
    """)
    opened_positions = cursor.fetchall()

    formatted_positions = []
    for pos in opened_positions:
        formatted_positions.append({
            'ID': pos[0],
            'Symbol': pos[1],
            'Timestamp': datetime.fromtimestamp(pos[2]),
            'Open Price': pos[3],
            'Side': pos[4],
            'Size': pos[5],
            'Take Profit': pos[6],
            'Stop Loss': pos[7],
            'Close Price': pos[8],
            'Close Time': datetime.fromtimestamp(pos[9]) if pos[9] else None,
            'Close Reason': pos[10]
        })
    return formatted_positions


def fetch_positions_opened_last_day():
    one_day_ago = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
    cursor.execute("""
    SELECT *
    FROM opened_positions
    WHERE timestamp >= ?
    """, (one_day_ago,))
    opened_positions = cursor.fetchall()

    formatted_positions = []
    for pos in opened_positions:
        formatted_positions.append({
            'ID': pos[0],
            'Symbol': pos[1],
            'Timestamp': datetime.fromtimestamp(pos[2]),
            'Open Price': pos[3],
            'Side': pos[4],
            'Size': pos[5],
            'Take Profit': pos[6],
            'Stop Loss': pos[7],
            'Close Price': pos[8],
            'Close Time': datetime.fromtimestamp(pos[9]) if pos[9] else None,
            'Close Reason': pos[10]
        })

    return formatted_positions


def print_positions(positions):
    for pos in positions:
        print(f"ID: {pos['ID']}")
        print(f"Symbol: {pos['Symbol']}")
        print(f"Timestamp: {pos['Timestamp']}")
        print(f"Open Price: {pos['Open Price']}")
        print(f"Side: {pos['Side']}")
        print(f"Size: {pos['Size']}")
        print(f"Take Profit: {pos['Take Profit']}")
        print(f"Stop Loss: {pos['Stop Loss']}")
        print(f"Close Price: {pos['Close Price']}")
        print(f"Close Time: {pos['Close Time']}")
        print(f"Close Reason: {pos['Close Reason']}")
        print("-" * 30)


"""__________________________________________________________________________________________________________________"""


print(calculate_stochastic_rsi(dollar_bars).tail(20))
print('\n')

# Fetch last n hours signals
# print(fetch_last_n_hours_signals(24))
# print('\n')

print(get_market_signal(dollar_bars,7, 3))
print('RSI : ', get_rsi(dollar_bars))


# Fetch last ten signals
# print(fetch_last_10_signals())

# Retrieve all open positions
# all_opened_positions = fetch_all_opened_positions()
# print_positions(all_opened_positions)

# Retrieve positions opened during the past day
positions_opened_last_day = fetch_positions_opened_last_day()
print_positions(positions_opened_last_day)


# Close the database connection
# conn.close()
