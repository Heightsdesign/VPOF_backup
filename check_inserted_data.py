import sqlite3
from datetime import datetime


# Function to convert Unix timestamp to readable datetime string
def unix_to_readable(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


# Connect to the SQLite database
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()
"""
# Fetch some volume profile data
cursor.execute("SELECT * FROM volume_profile")
volume_profile_data = cursor.fetchall()
print("Volume Profile Data:")
for row in volume_profile_data:
    readable_timestamp = unix_to_readable(row[1] * 4 * 3600)  # Convert interval to readable time
    print((row[0], readable_timestamp, row[2], row[3]))
"""
# Fetch some trade data
cursor.execute("SELECT * FROM trades")
trade_data = cursor.fetchall()
print("Trade Data:")
for row in trade_data:
    readable_timestamp = unix_to_readable(row[1])
    print((row[0], readable_timestamp, row[2], row[3], row[4], row[5]))

# Close the database connection"""
conn.close()



