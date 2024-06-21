import sqlite3

# Connect to SQLite database (it will create the database file if it doesn't exist)
conn = sqlite3.connect('trading_data.db')
cursor = conn.cursor()

create_trades_table = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    price REAL,
    volume REAL,
    side TEXT,
    type_order TEXT
);
"""

# Create volume profile table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS volume_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interval INTEGER,
    price_level REAL,
    volume REAL)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    order_flow_signal TEXT,
    order_flow_score INTEGER,
    volume_profile_signal TEXT,
    price_action_signal TEXT
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS opened_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    timestamp INTEGER,
    open_price REAL,
    side TEXT,
    size REAL,
    take_profit REAL,
    stop_loss REAL,
    close_price REAL,
    close_time INTEGER
)

""")


# Execute SQL commands to create tables
cursor.execute(create_trades_table)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Tables created successfully.")
