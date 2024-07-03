import pandas as pd
import sqlite3
from datetime import datetime, timedelta


# Database connection
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()


def fetch_trades(hours=48):
    current_time = datetime.now()
    start_time = current_time - timedelta(hours=hours)
    start_timestamp = int(start_time.timestamp())

    cursor.execute("""
    SELECT timestamp, price, volume, side, type_order
    FROM trades
    WHERE timestamp >= ?
    ORDER BY timestamp ASC
    """, (start_timestamp,))

    trades = cursor.fetchall()

    trade_data = pd.DataFrame(trades, columns=['timestamp', 'price', 'volume', 'side', 'type_order'])
    trade_data['timestamp'] = pd.to_datetime(trade_data['timestamp'], unit='s')

    return trade_data


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


"""__________________________________________________________________________________________________________________"""


# Fetch trades and create dollar bars
trade_data = fetch_trades(hours=48)
dollar_bars = create_dollar_bars(trade_data, dollar_threshold=2500000)