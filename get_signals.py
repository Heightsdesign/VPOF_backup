import numpy as np
import sqlite3
from datetime import datetime, timezone, timedelta

from order_flow_tools import calculate_order_flow_metrics

# Ensure the database connection is open
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()


(delta_values, cumulative_delta, min_delta_values,
 max_delta_values, market_buy_ratios, market_sell_ratios,
 buy_volumes, sell_volumes, aggressive_buy_activities,
 aggressive_sell_activities, aggressive_ratios) = calculate_order_flow_metrics()


def get_spikes(data):
    signals = []

    # Turn all values positive for median calculation
    abs_data = [abs(value) for value in data]

    # Calculate the median value
    median_val = np.median(abs_data)

    # Iterate over the data and determine signals based on thresholds
    for i, value in enumerate(data):
        abs_value = abs(value)
        if abs_value > median_val * 6:
            rating = 'very_strong'
        elif abs_value > median_val * 4:
            rating = 'strong'
        elif abs_value > median_val * 2:
            rating = 'mild'
        else:
            rating = None

        if rating:
            signals.append((i, value, rating))

    return signals


def market_sentiment_eval(signals):

    pressure = 0
    rating = None

    for signal in signals:
        pressure += signal[1]

    if pressure >= len(signals) * 4:
        rating = "buy"
    elif len(signals) * 4 > pressure > len(signals) * -4:
        rating = "hold"
    elif pressure <= len(signals) * -4:
        rating = "sell"

    print("Pressure : ", pressure)
    print("Market Sentiment : ", rating)

    return [pressure, rating]


# Function to score signals and generate a final decision
def generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold):
    rating_score = {
        'mild': 1,
        'strong': 2,
        'very_strong': 3,
    }

    pos_ratios = 0
    neg_ratios = 0

    def score_signals(signals):
        score = 0
        for i, value, rating in signals:
            signal_score = rating_score[rating]
            if value < 0:
                signal_score = -signal_score
            score += signal_score
        return score

    for ratio in aggressive_ratios:
        if ratio > 0:
            pos_ratios += 1
        elif ratio < 0:
            neg_ratios += 1

    aggressive_ratio_score = score_signals(aggressive_ratio_signals)
    delta_value_score = score_signals(delta_value_signals)

    total_score = aggressive_ratio_score + delta_value_score

    if cumulative_delta > 0:
        total_score += 2
    elif cumulative_delta < 0:
        total_score -= 2

    if pos_ratios > neg_ratios:
        total_score += 2
    elif pos_ratios < neg_ratios:
        total_score -= 2

    print('SCORE :', total_score)

    if total_score >= threshold:
        return ['buy', total_score]
    elif total_score <= -threshold:
        return ['sell', total_score]
    else:
        return ['hold', total_score]


# Function to fetch and display stored signals
def fetch_and_display_signals():
    cursor.execute("SELECT * FROM signals ORDER BY timestamp DESC")
    signals = cursor.fetchall()

    print("Stored Signals:")
    for signal in signals:
        signal_id, timestamp, order_flow_signal, orderflow_score, market_pressure, volume_profile_signal, price_action_signal = signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Order Flow Score: {orderflow_score}, Market Pressure: {market_pressure}")

    return signals


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


# Function to fetch all signals from the past 24 hours
def fetch_last_24_hours_signals():
    current_time = datetime.now(timezone.utc)
    start_timestamp = int((current_time - timedelta(hours=24)).timestamp())

    cursor.execute("""
    SELECT * FROM signals 
    WHERE timestamp >= ? 
    ORDER BY timestamp DESC
    """, (start_timestamp,))
    last_24_hours_signals = cursor.fetchall()

    print("Signals from the Past 24 Hours:")
    for signal in last_24_hours_signals:
        signal_id, timestamp, order_flow_signal, orderflow_score, market_pressure, volume_profile_signal, price_action_signal = signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Order Flow Score: {orderflow_score}, Market Pressure: {market_pressure}")

    return last_24_hours_signals


def fetch_last_4_hours_signals():
    current_time = datetime.now(timezone.utc)
    start_timestamp = int((current_time - timedelta(hours=4)).timestamp())

    cursor.execute("""
    SELECT order_flow_signal, order_flow_score FROM signals 
    WHERE timestamp >= ? 
    ORDER BY timestamp DESC
    """, (start_timestamp,))
    last_4_hours_signals = cursor.fetchall()

    return last_4_hours_signals


def fetch_last_10_signals():
    cursor.execute("""
    SELECT order_flow_signal, order_flow_score FROM signals 
    ORDER BY timestamp DESC
    LIMIT 10
    """)
    return cursor.fetchall()


"""__________________________________________________________________________________________________________________"""

# Example usage with your data
# aggressive_buy_activities = [0.011, 0.001, 0.008, 0.076, 0.096, 0.33, 0.168, 0.005, 0.004, 0.0, 0.001, 0.102]
# aggressive_sell_activities = [70.004, 50.0, 0.003, 0.007, 0.005, 0.002, 1.425, 0.007, 0.003, 0.0, 0.012, 0.0]

# Generate the signals
aggressive_ratio_signals = get_spikes(aggressive_ratios)
delta_value_signals = get_spikes(delta_values)

# Fetch and display the signals
# fetch_and_display_signals()

print(f"Aggressive Ratio Signals: {aggressive_ratio_signals}")
print(f"Delta Value Signals: {delta_value_signals}")
# Generate final signal
final_signal = generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold=10)
print(f"Final Signal: {final_signal}")

# Fetch and display the last 'buy' and 'sell' signals
fetch_last_buy_signal()
fetch_last_sell_signal()

# Fetch and display today's signals
fetch_last_24_hours_signals()

# Fetch last 4 hours signals
# print(fetch_last_4_hours_signals())

# Fetch last ten signals
# print(fetch_last_10_signals())

# Close the database connection
# conn.close()
