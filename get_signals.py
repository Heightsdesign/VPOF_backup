import numpy as np
import sqlite3
from datetime import datetime, timezone

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
    base_threshold = 4

    # Turn all values positive for median calculation
    abs_data = [abs(value) for value in data]

    # Calculate the median value
    median_val = np.median(abs_data)

    # Iterate over the data and determine signals based on thresholds
    for i, value in enumerate(data):
        abs_value = abs(value)
        if abs_value > median_val * base_threshold * 6:
            rating = 'fire at will'
        elif abs_value > median_val * base_threshold * 4:
            rating = 'very_strong'
        elif abs_value > median_val * base_threshold * 2:
            rating = 'strong'
        elif abs_value > median_val * base_threshold:
            rating = 'mild'
        else:
            rating = None

        if rating:
            signals.append((i, value, rating))

    return signals


# Function to score signals and generate a final decision
def generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold=4):
    rating_score = {
        'mild': 1,
        'strong': 2,
        'very_strong': 3,
        'fire at will': 4
    }

    def score_signals(signals):
        score = 0
        for i, value, rating in signals:
            signal_score = rating_score[rating]
            if value < 0:
                signal_score = -signal_score
            score += signal_score
        return score

    aggressive_ratio_score = score_signals(aggressive_ratio_signals)
    delta_value_score = score_signals(delta_value_signals)

    total_score = aggressive_ratio_score + delta_value_score

    if cumulative_delta > 0:
        total_score += 2
    elif cumulative_delta < 0:
        total_score -= 2

    if total_score >= threshold:
        return 'buy'
    elif total_score <= -threshold:
        return 'sell'
    else:
        return 'hold'


# Function to fetch and display stored signals
def fetch_and_display_signals():
    cursor.execute("SELECT * FROM signals ORDER BY timestamp DESC")
    signals = cursor.fetchall()

    print("Stored Signals:")
    for signal in signals:
        signal_id, timestamp, order_flow_signal, volume_profile_signal, price_action_signal = signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Volume Profile Signal: {volume_profile_signal}, Price Action Signal: {price_action_signal}")


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
        signal_id, timestamp, order_flow_signal, volume_profile_signal, price_action_signal = last_buy_signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Last Buy Signal - ID: {signal_id}, Timestamp: {readable_timestamp}, "
              f"Order Flow Signal: {order_flow_signal}, Volume Profile Signal: {volume_profile_signal}, "
              f"Price Action Signal: {price_action_signal}")
    else:
        print("No 'buy' signals found.")


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
        signal_id, timestamp, order_flow_signal, volume_profile_signal, price_action_signal = last_sell_signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Last Sell Signal - ID: {signal_id}, Timestamp: {readable_timestamp}, "
              f"Order Flow Signal: {order_flow_signal}, Volume Profile Signal: {volume_profile_signal}, "
              f"Price Action Signal: {price_action_signal}")
    else:
        print("No 'sell' signals found.")


# Function to fetch all signals from today
def fetch_today_signals():
    current_time = datetime.now(timezone.utc)
    start_of_day = datetime(current_time.year, current_time.month, current_time.day, tzinfo=timezone.utc)
    start_timestamp = int(start_of_day.timestamp())

    cursor.execute("""
    SELECT * FROM signals 
    WHERE timestamp >= ? 
    ORDER BY timestamp DESC
    """, (start_timestamp,))
    today_signals = cursor.fetchall()

    print("Today's Signals:")
    for signal in today_signals:
        signal_id, timestamp, order_flow_signal, volume_profile_signal, price_action_signal = signal
        readable_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ID: {signal_id}, Timestamp: {readable_timestamp}, Order Flow Signal: {order_flow_signal}, "
              f"Volume Profile Signal: {volume_profile_signal}, Price Action Signal: {price_action_signal}")


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
fetch_today_signals()

# Close the database connection
conn.close()
