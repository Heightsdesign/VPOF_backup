import sqlite3
from datetime import timedelta
import numpy as np
from sklearn.linear_model import LinearRegression

from dollar_bars import dollar_bars

# Variables
time_frame_minutes = 5  # Adjust this variable as needed
look_back_period = 10  # Number of candles to look back

# Connect to the SQLite database
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()


# Create the table if it does not exist
def create_tables():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deltas (
        start_time INTEGER PRIMARY KEY,
        end_time INTEGER,
        total_delta REAL,
        min_delta REAL,
        max_delta REAL,
        buy_volume REAL,
        sell_volume REAL
    )
    """)
    conn.commit()


create_tables()
def calculate_order_flow_metrics(dol_bars):
    delta_values = []
    cumulative_delta = 0
    min_delta_values = []
    max_delta_values = []
    market_buy_ratios = []
    market_sell_ratios = []
    buy_volumes = []
    sell_volumes = []
    aggressive_buy_activities = []
    aggressive_sell_activities = []
    aggressive_ratios = []
    aggressive_ratio = 0

    for i in range(len(dol_bars)):
        bar = dol_bars.iloc[i]
        start_time = bar['start_time']
        end_time = bar['end_time']

        cursor.execute("""
            SELECT side, volume, type_order
            FROM trades
            WHERE timestamp BETWEEN ? AND ?
            """, (int(start_time.timestamp()), int(end_time.timestamp())))

        trades = cursor.fetchall()

        buy_volume = 0
        sell_volume = 0
        market_buy_volume = 0
        market_sell_volume = 0
        delta = 0
        min_delta = float('inf')
        max_delta = float('-inf')

        for side, volume, type_order in trades:
            if side == 'buy':
                buy_volume += volume
                delta += volume
                if type_order == 'market':
                    market_buy_volume += volume
            elif side == 'sell':
                sell_volume += volume
                delta -= volume
                if type_order == 'market':
                    market_sell_volume += volume
            min_delta = min(min_delta, delta)
            max_delta = max(max_delta, delta)

        # Debug: Print calculated values for each bar
        # print(f"Bar {i} values:")
        # print(f"Buy Volume: {buy_volume}, Sell Volume: {sell_volume}, Delta: {delta}, Min Delta: {min_delta}, Max Delta: {max_delta}")

        total_delta = buy_volume - sell_volume

        cumulative_delta += total_delta

        delta_values.append(round(total_delta, 2))
        min_delta_values.append(round(min_delta, 2))
        max_delta_values.append(round(max_delta, 2))
        buy_volumes.append(round(buy_volume, 2))
        sell_volumes.append(round(sell_volume, 2))

        market_buy_ratio = market_buy_volume / (market_buy_volume + buy_volume) if (market_buy_volume + buy_volume) > 0 else 0
        market_sell_ratio = market_sell_volume / (market_sell_volume + sell_volume) if (market_sell_volume + sell_volume) > 0 else 0

        market_buy_ratios.append(round(market_buy_ratio, 2))
        market_sell_ratios.append(round(market_sell_ratio, 2))

        aggressive_buy_activity = round(market_buy_ratio * buy_volume, 3)
        aggressive_sell_activity = round(market_sell_ratio * sell_volume, 3)
        aggressive_buy_activities.append(aggressive_buy_activity)
        aggressive_sell_activities.append(aggressive_sell_activity)

        if aggressive_buy_activity > aggressive_sell_activity > 0:
            aggressive_ratio = aggressive_buy_activity / aggressive_sell_activity
        elif aggressive_sell_activity > aggressive_buy_activity > 0:
            aggressive_ratio = (aggressive_sell_activity / aggressive_buy_activity) * -1

        aggressive_ratios.append(round(aggressive_ratio, 3))

    latest_bar = dol_bars.iloc[-1].copy()  # Create a copy to avoid SettingWithCopyWarning
    latest_bar['total_delta'] = total_delta
    latest_bar['min_delta'] = min_delta
    latest_bar['max_delta'] = max_delta
    latest_bar['buy_volume'] = buy_volume
    latest_bar['sell_volume'] = sell_volume

    # Debug: Print final latest bar values
    print("Latest bar values:")
    print(latest_bar)

    return (delta_values, cumulative_delta, min_delta_values,
            max_delta_values, market_buy_ratios, market_sell_ratios,
            buy_volumes, sell_volumes, aggressive_buy_activities,
            aggressive_sell_activities, aggressive_ratios, latest_bar)


def insert_latest_delta(latest_bar):
    # Check if the entry already exists in the database
    cursor.execute("""
    SELECT 1 FROM deltas WHERE start_time = ?
    """, (int(latest_bar['timestamp'].timestamp()),))
    if cursor.fetchone() is None:
        cursor.execute("""
        INSERT INTO deltas (start_time, end_time, total_delta, min_delta, max_delta, buy_volume, sell_volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (int(latest_bar['timestamp'].timestamp()), int(latest_bar['timestamp'].timestamp()),
              latest_bar['total_delta'], latest_bar['min_delta'], latest_bar['max_delta'],
              latest_bar['buy_volume'], latest_bar['sell_volume']))
        conn.commit()


def calculate_slope(values):
    x = np.arange(len(values)).reshape(-1, 1)
    y = np.array(values).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)
    slope = model.coef_[0][0]
    return slope


# Calculate order flow metrics using dollar bars
(delta_values, cumulative_delta, min_delta_values,
 max_delta_values, market_buy_ratios, market_sell_ratios,
 buy_volumes, sell_volumes, aggressive_buy_activities,
 aggressive_sell_activities, aggressive_ratios, latest_bar) = calculate_order_flow_metrics(dollar_bars)

# Insert the latest delta values into the database
# insert_latest_delta(latest_bar)

# Output metrics
print(f"Delta Values: {delta_values}")
print(f"Cumulative Delta: {cumulative_delta}")
print(f"Min Delta Values: {min_delta_values}")
print(f"Max Delta Values: {max_delta_values}")
print(f"Market Buy Ratios: {market_buy_ratios}")
print(f"Market Sell Ratios: {market_sell_ratios}")
print(f"Buy Volumes: {buy_volumes}")
print(f"Sell Volumes: {sell_volumes}")
print(f"Aggressive Buy Activities: {aggressive_buy_activities}")
print(f"Aggressive Sell Activities: {aggressive_sell_activities}")
print(f"Aggressive Ratios: {aggressive_ratios}")

# Calculate slope of aggressive ratios
slope_of_aggressive_ratios = calculate_slope(aggressive_ratios)
print(f"Slope of Aggressive Ratios: {slope_of_aggressive_ratios}")

# Close the database connection
# conn.close()
