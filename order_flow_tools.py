import sqlite3
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.linear_model import LinearRegression


# Variables
time_frame_minutes = 5  # Adjust this variable as needed
look_back_period = 7  # Number of candles to look back

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


# Function to align the current time to the nearest past 5-minute interval
def align_to_interval(current_time, interval_minutes):
    return current_time - timedelta(minutes=current_time.minute % interval_minutes,
                                    seconds=current_time.second,
                                    microseconds=current_time.microsecond)


# Function to calculate delta and track min/max deltas within a timeframe
def calculate_delta_and_extremes(start_time, end_time, previous_cumulative_delta):
    cursor.execute("""
    SELECT total_delta, min_delta, max_delta, buy_volume, sell_volume
    FROM deltas
    WHERE start_time = ?
    """, (start_time,))

    row = cursor.fetchone()

    if row:
        total_delta, min_delta, max_delta, buy_volume, sell_volume = row
    else:
        cursor.execute("""
        SELECT side, volume, type_order
        FROM trades 
        WHERE timestamp BETWEEN ? AND ?
        """, (start_time, end_time))

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

        total_delta = buy_volume - sell_volume

        # Store the calculated values in the deltas table
        cursor.execute("""
        INSERT INTO deltas (start_time, end_time, total_delta, min_delta, max_delta, buy_volume, sell_volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (start_time, end_time, total_delta, min_delta, max_delta, buy_volume, sell_volume))
        conn.commit()

    cumulative_delta = previous_cumulative_delta + total_delta

    return total_delta, min_delta, max_delta, cumulative_delta, buy_volume, sell_volume


# Function to calculate metrics for a given timeframe
# Function to calculate metrics for a given timeframe
def calculate_order_flow_metrics():
    current_time = datetime.now(timezone.utc)
    aligned_end_time = align_to_interval(current_time, time_frame_minutes)
    end_time = int(aligned_end_time.timestamp())
    start_time = end_time - (time_frame_minutes * 60)

    delta_values = []
    cumulative_delta = 0  # Initialize cumulative delta
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

    # Process data from the oldest to the newest
    for _ in range(look_back_period):
        start_time -= (time_frame_minutes * 60)
        end_time = start_time + (time_frame_minutes * 60)
        total_delta, min_delta, max_delta, cumulative_delta, buy_volume, sell_volume = calculate_delta_and_extremes(start_time, end_time, cumulative_delta)
        delta_values.append(round(total_delta, 2))
        min_delta_values.append(round(min_delta, 2))
        max_delta_values.append(round(max_delta, 2))
        buy_volumes.append(round(buy_volume, 2))
        sell_volumes.append(round(sell_volume, 2))

        # Calculate market buy and sell ratios
        cursor.execute("""
        SELECT 
            SUM(CASE WHEN side = 'buy' AND type_order = 'market' THEN volume ELSE 0 END) AS market_buys,
            SUM(CASE WHEN side = 'buy' AND type_order = 'limit' THEN volume ELSE 0 END) AS limit_buys,
            SUM(CASE WHEN side = 'sell' AND type_order = 'market' THEN volume ELSE 0 END) AS market_sells,
            SUM(CASE WHEN side = 'sell' AND type_order = 'limit' THEN volume ELSE 0 END) AS limit_sells
        FROM trades
        WHERE timestamp BETWEEN ? AND ?
        """, (start_time, end_time))

        result = cursor.fetchone()
        market_buys = result[0] if result[0] is not None else 0
        limit_buys = result[1] if result[1] is not None else 0
        market_sells = result[2] if result[2] is not None else 0
        limit_sells = result[3] if result[3] is not None else 0

        market_buy_ratio = market_buys / (market_buys + limit_buys) if (market_buys + limit_buys) > 0 else 0
        market_sell_ratio = market_sells / (market_sells + limit_sells) if (market_sells + limit_sells) > 0 else 0

        market_buy_ratios.append(round(market_buy_ratio, 2))
        market_sell_ratios.append(round(market_sell_ratio, 2))

        # Calculate aggressive activities
        aggressive_buy_activity = round(market_buy_ratio * buy_volume, 3)
        aggressive_sell_activity = round(market_sell_ratio * sell_volume, 3)
        aggressive_buy_activities.append(aggressive_buy_activity)
        aggressive_sell_activities.append(aggressive_sell_activity)

        # Calculate aggressive activity ratio
        if aggressive_buy_activity > aggressive_sell_activity > 0:
            aggressive_ratio = aggressive_buy_activity / aggressive_sell_activity
        elif aggressive_sell_activity > aggressive_buy_activity > 0:
            aggressive_ratio = (aggressive_sell_activity / aggressive_buy_activity) * -1

        aggressive_ratios.append(round(aggressive_ratio, 3))

    return (delta_values, cumulative_delta, min_delta_values,
            max_delta_values, market_buy_ratios, market_sell_ratios,
            buy_volumes, sell_volumes, aggressive_buy_activities,
            aggressive_sell_activities, aggressive_ratios)


# Function to calculate slope of aggressive ratios
def calculate_slope(values):
    x = np.arange(len(values)).reshape(-1, 1)
    y = np.array(values).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)
    slope = model.coef_[0][0]
    return slope


"""__________________________________________________________________________________________________________________"""
print('\n')


# Fetching and printing the metrics
(delta_values, cumulative_delta, min_delta_values,
 max_delta_values, market_buy_ratios, market_sell_ratios,
 buy_volumes, sell_volumes, aggressive_buy_activities,
 aggressive_sell_activities, aggressive_ratios) = calculate_order_flow_metrics()

print(f"Delta Values: {delta_values}")
print(f"Cumulative Delta: {cumulative_delta}")
print(f"Min Delta Values: {min_delta_values}")
print(f"Max Delta Values: {max_delta_values}")
print('\n')
print(f"Market Buy Ratios: {market_buy_ratios}")
print(f"Market Sell Ratios: {market_sell_ratios}")
print(f"Buy Volumes: {buy_volumes}")
print(f"Sell Volumes: {sell_volumes}")
print('\n')
print(f"Aggressive Buy Activities: {aggressive_buy_activities}")
print(f"Aggressive Sell Activities: {aggressive_sell_activities}")
print(f"Aggressive Ratios: {aggressive_ratios}")

# Calculate and print the slope of the aggressive ratios
slope_of_aggressive_ratios = calculate_slope(aggressive_ratios)
print(f"Slope of Aggressive Ratios: {slope_of_aggressive_ratios}")
print('\n')

# Close the database connection
# conn.close()