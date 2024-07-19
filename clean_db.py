import sqlite3
from datetime import datetime, timezone, timedelta

# Connect to the SQLite database
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()

# Get the current time and calculate the timestamp for one week ago
current_time = datetime.now(timezone.utc)
one_week_ago = current_time - timedelta(days=30)
one_week_ago_timestamp = int(one_week_ago.timestamp())

# SQL query to delete trades older than one week
delete_query = """
DELETE FROM trades
WHERE timestamp < ?
"""

# Execute the delete query
cursor.execute(delete_query, (one_week_ago_timestamp,))
conn.commit()

# Verify deletion
cursor.execute("SELECT COUNT(*) FROM trades WHERE timestamp < ?", (one_week_ago_timestamp,))
count = cursor.fetchone()[0]
if count == 0:
    print("Successfully deleted trades older than one week.")
else:
    print(f"Failed to delete some old trades. {count} old trades remaining.")

# Close the database connection
conn.close()
