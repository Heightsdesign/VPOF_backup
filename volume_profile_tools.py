import requests
import numpy as np
from datetime import datetime, timezone

# Kraken API URL
kraken_api_url = 'https://api.kraken.com/0/public/OHLC'

# Variables
look_back_period_hours = 12  # Number of hours to look back
pair = 'XXBTZUSD'  # Correct currency pair for BTC/USD
interval = 1 # Minute interval
volume_profile_results = {}


# Function to fetch minute bars from Kraken
def fetch_minute_bars(pair, interval, look_back_period_hours):
    end_time = int(datetime.now(timezone.utc).timestamp())
    start_time = end_time - (look_back_period_hours * 3600)

    params = {
        'pair': pair,
        'interval': interval,
        'since': start_time
    }

    response = requests.get(kraken_api_url, params=params)
    data = response.json()

    if not data['error']:
        return data['result'][pair]
    else:
        raise Exception(f"Error fetching data from Kraken API: {data['error']}")


# Function to calculate the volume profile
def calculate_volume_profile(minute_bars):
    volume_profile = {}

    for bar in minute_bars:
        timestamp, open_, high, low, close, vwap, volume, count = map(float, bar[:8])
        direction = 'up' if close >= open_ else 'down'
        low = int(low)
        high = int(high)
        if high != low:
            volume_per_price = volume / (high - low + 1)
            for price in range(low, high + 1):
                if price not in volume_profile:
                    volume_profile[price] = {'up': 0, 'down': 0}
                volume_profile[price][direction] += volume_per_price
        else:
            if low not in volume_profile:
                volume_profile[low] = {'up': 0, 'down': 0}
            volume_profile[low][direction] += volume

    return volume_profile


# Fetching minute bars
minute_bars = fetch_minute_bars(pair, interval, look_back_period_hours)

# Calculating volume profile
volume_profile = calculate_volume_profile(minute_bars)

# Calculate the median volume and define an initial threshold
volumes = [volumes['up'] + volumes['down'] for price, volumes in volume_profile.items()]
initial_median_volume = np.median(volumes)
initial_threshold = initial_median_volume * 3

# Identify initial clusters
initial_clusters = []
current_cluster = []

for price in sorted(volume_profile.keys()):
    total_volume = volume_profile[price]['up'] + volume_profile[price]['down']
    if total_volume >= initial_threshold:
        current_cluster.append((price, total_volume))
    else:
        if current_cluster:
            initial_clusters.append(current_cluster)
            current_cluster = []

if current_cluster:
    initial_clusters.append(current_cluster)

# Calculate properties for initial clusters
cluster_properties = []

for cluster in initial_clusters:
    start_price = cluster[0][0]
    end_price = cluster[-1][0]
    poc_price, poc_volume = max(cluster, key=lambda x: x[1])
    total_volume = sum(volume for price, volume in cluster)
    cluster_properties.append({
        "start_price": start_price,
        "end_price": end_price,
        "poc_price": poc_price,
        "poc_volume": poc_volume,
        "total_volume": total_volume
    })

# Calculate median volume of the clusters
cluster_volumes = [cluster['total_volume'] for cluster in cluster_properties]
cluster_median_volume = np.median(cluster_volumes) * 3

# Filter clusters based on the new median volume
filtered_clusters = [cluster for cluster in cluster_properties if cluster['total_volume'] >= cluster_median_volume]
print(filtered_clusters)

print('\n')
# Define 'The Zone'
if filtered_clusters:
    zone_start = filtered_clusters[0]['start_price']
    zone_end = filtered_clusters[-1]['end_price']
    print(f"The Zone: Start Price: {zone_start}, End Price: {zone_end}")
    volume_profile_results['start'] = zone_start
    volume_profile_results['end'] = zone_end

    # Identify the POC of 'The Zone'
    zone_poc_cluster = max(filtered_clusters, key=lambda x: x['total_volume'])
    zone_poc_price = zone_poc_cluster['poc_price']
    zone_poc_volume = zone_poc_cluster['poc_volume']
    print(f"Zone POC: Price: {zone_poc_price}, Volume: {zone_poc_volume}")

    # Divide 'The Zone' into five segments
    zone_range = zone_end - zone_start
    segment_size = zone_range / 6

    segment_volumes = [0] * 6

    for price, volumes in volume_profile.items():
        total_volume = volumes['up'] + volumes['down']
        if zone_start <= price <= zone_end:
            segment_index = int((price - zone_start) // segment_size)
            if segment_index >= 6:
                segment_index = 5
            segment_volumes[segment_index] += total_volume

    for i in range(6):
        print(f"Segment {i+1} Volume: {segment_volumes[i]}")
    print('\n')

# Print the filtered cluster properties
"""for idx, cluster in enumerate(filtered_clusters):
    print(f"Cluster {idx+1}:")
    print(f"  Start Price: {cluster['start_price']}")
    print(f"  End Price: {cluster['end_price']}")
    print(f"  POC Price: {cluster['poc_price']}")
    print(f"  POC Volume: {cluster['poc_volume']}")
    print(f"  Total Volume: {cluster['total_volume']}")"""
