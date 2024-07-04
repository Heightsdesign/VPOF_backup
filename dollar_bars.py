import pandas as pd
import sqlite3
from datetime import datetime, timedelta


# Database connection
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()


def fetch_trades(hours=7):
    # Calculate the timestamp for the starting point
    current_time = datetime.now()
    start_time = current_time - timedelta(hours=hours)
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

    # print(trade_data.head())  # Print first few rows of trade data for verification

    return trade_data


def create_dollar_bars(trade_data, dollar_threshold):
    # Check if trade_data is empty
    if trade_data.empty:
        print("No trade data available.")
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'dollar_volume', 'start_time', 'end_time'])

    dollar_bars = []
    temp_dollar = 0
    open_price = trade_data['price'].iloc[0]
    high_price = trade_data['price'].iloc[0]
    low_price = trade_data['price'].iloc[0]
    close_price = trade_data['price'].iloc[0]
    start_time = trade_data['timestamp'].iloc[0]

    for index, row in trade_data.iterrows():
        trade_dollar = row['price'] * row['volume']
        temp_dollar += trade_dollar
        high_price = max(high_price, row['price'])
        low_price = min(low_price, row['price'])
        close_price = row['price']
        end_time = row['timestamp']

        if temp_dollar >= dollar_threshold:
            dollar_bars.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'dollar_volume': temp_dollar,
                'start_time': start_time,
                'end_time': end_time
            })
            temp_dollar = 0
            open_price = row['price']
            high_price = row['price']
            low_price = row['price']
            start_time = end_time

    return pd.DataFrame(dollar_bars)


"""__________________________________________________________________________________________________________________"""

# Fetch trades and create dollar bars
trade_data = fetch_trades(hours=7)
dollar_bars = create_dollar_bars(trade_data, dollar_threshold=2500000)