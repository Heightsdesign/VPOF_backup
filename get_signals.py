import numpy as np
import sqlite3
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


"""__________________________________________________________________________________________________________________"""

# Example usage with your data
# aggressive_buy_activities = [0.011, 0.001, 0.008, 0.076, 0.096, 0.33, 0.168, 0.005, 0.004, 0.0, 0.001, 0.102]
# aggressive_sell_activities = [70.004, 50.0, 0.003, 0.007, 0.005, 0.002, 1.425, 0.007, 0.003, 0.0, 0.012, 0.0]

# Generate the signals
aggressive_ratio_signals = get_spikes(aggressive_ratios)
delta_value_signals = get_spikes(delta_values)

# Close the database connection after calculations are done
conn.close()

print(f"Aggressive Ratio Signals: {aggressive_ratio_signals}")
print(f"Delta Value Signals: {delta_value_signals}")
# Generate final signal
final_signal = generate_final_signal(aggressive_ratio_signals, delta_value_signals, cumulative_delta, threshold=8)
print(f"Final Signal: {final_signal}")
