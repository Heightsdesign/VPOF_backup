import base64, hashlib, hmac
import requests
import urllib.parse
import constants
import datetime
import time
import json
import pandas as pd


# Define the base API URL for Kraken Futures
api_url = 'https://futures.kraken.com/derivatives/api/v3'


class KrakenFuturesAuth:
    def __init__(self, api_key, api_secret, endpoint):
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = endpoint

    def generate_signature(self, postData):

        # No longer using nonce
        message = postData + self.endpoint
        # print("Message for signature:", message)  # Debugging

        sha256_hash = hashlib.sha256()
        sha256_hash.update(message.encode())

        secret_key = base64.b64decode(self.api_secret)
        hmac_sha512 = hmac.new(secret_key, sha256_hash.digest(), hashlib.sha512)
        signature = base64.b64encode(hmac_sha512.digest()).decode().strip()

        # print("Generated signature:", signature)  # Debugging
        return signature

    def __call__(self, request):

        postData = request.body if request.body else ''
        # print("Request body (postData):", postData)  # Debugging
        # print("Endpoint path:", self.endpoint)  # Debugging

        signature = self.generate_signature(postData)
        request.headers.update({
            "APIKey": self.api_key,
            "Authent": signature
        })
        # print("Request headers:", request.headers)  # Debugging
        return request


def fetch_candles_since(pair, interval=5, start_time=None):
    """
    Fetch OHLC candles from Kraken starting from a specific timestamp.

    :param pair: The trading pair (e.g., 'XXBTZUSD' for BTC/USD).
    :param interval: The interval in minutes (1, 5, 15, 30, 60, 240, 1440, 10080, 21600).
    :param start_time: The starting timestamp in seconds.
    :return: DataFrame containing the OHLC data.
    """
    url = 'https://api.kraken.com/0/public/OHLC'
    params = {
        'pair': pair,
        'interval': interval,
        'since': start_time
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data['error']:
        raise Exception(f"Error fetching data from Kraken API: {data['error']}")

    ohlc_data = data['result'][pair]
    df = pd.DataFrame(ohlc_data, columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])

    # Convert timestamp to datetime
    df['time'] = pd.to_datetime(df['time'], unit='s')

    # Ensure numeric types for calculations
    df[['open', 'high', 'low', 'close', 'vwap', 'volume']] = df[
        ['open', 'high', 'low', 'close', 'vwap', 'volume']].apply(pd.to_numeric)

    return df


def fetch_last_n_candles(pair, interval=5, num_candles=60):
    """
    Fetch the last N OHLC candles from Kraken.

    :param pair: The trading pair (e.g., 'XXBTZUSD' for BTC/USD).
    :param interval: The interval in minutes (1, 5, 15, 30, 60, 240, 1440, 10080, 21600).
    :param num_candles: The number of candles to fetch.
    :return: DataFrame containing the OHLC data.
    """
    url = 'https://api.kraken.com/0/public/OHLC'
    params = {
        'pair': pair,
        'interval': interval
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data['error']:
        raise Exception(f"Error fetching data from Kraken API: {data['error']}")

    ohlc_data = data['result'][pair][-num_candles:]
    df = pd.DataFrame(ohlc_data, columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])

    # Convert timestamp to datetime
    df['time'] = pd.to_datetime(df['time'], unit='s')

    # Ensure numeric types for calculations
    df[['open', 'high', 'low', 'close', 'vwap', 'volume']] = df[
        ['open', 'high', 'low', 'close', 'vwap', 'volume']].apply(pd.to_numeric)

    return df


def place_order(auth, symbol, side, size, orderType='mkt', limitPrice=None, stopPrice=None, clientOrderId=None):
    endpoint = '/sendorder'
    full_url = api_url + endpoint

    order = {
        'orderType': orderType,
        'symbol': symbol,
        'side': side,
        'size': size
    }
    if stopPrice is not None:
        order['stopPrice'] = stopPrice

    if limitPrice is not None:
        order['limitPrice'] = limitPrice

    if clientOrderId is not None:
        order['cliOrdId'] = clientOrderId

    print(order)

    postBody = urllib.parse.urlencode(order)
    response = requests.post(full_url, data=postBody, auth=auth,
                             headers={'Content-Type': 'application/x-www-form-urlencoded'})

    print(response.json())
    return response.json()


def get_open_positions(auth):

    endpoint = '/openpositions'
    full_url = api_url + endpoint

    payload = {}
    headers = {
        'Accept': 'application/json',
    }

    response = requests.get(full_url, auth=auth, headers=headers, data=payload)
    return response.json()


def get_open_fills(auth):

    endpoint = '/fills'
    full_url = api_url + endpoint

    payload = {}
    headers = {
        'Accept': 'application/json',
    }

    response = requests.get(full_url, auth=auth, headers=headers, data=payload)
    return response.json()


def get_account_info(auth):
    endpoint = '/accounts'  # Replace with the correct endpoint for account information
    full_url = api_url + endpoint

    response = requests.get(full_url, auth=auth, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    return response.json()


def get_instruments():
    url = "https://futures.kraken.com/derivatives/api/v3/instruments"

    payload = {}
    headers = {
        'Accept': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()


def get_future_price(pair):

    url = "https://api.kraken.com/0/public/Ticker"
    params = {
        'pair': pair
    }
    response = requests.get(url, params=params)
    data = response.json()
    print(data)
    return data['result'][pair]['c'][0]


def fetch_live_price(symbol):
    url = f"https://futures.kraken.com/derivatives/api/v3/tickers"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Search for the specific symbol in the data
        for item in data.get('tickers', []):
            if item['symbol'] == symbol:
                return {
                    'last_price': item['last'],
                    'bid': item['bid'],
                    'ask': item['ask']
                }
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None


def get_open_orders(auth):

    endpoint = '/openorders'
    full_url = api_url + endpoint

    payload = {}
    headers = {
        'Accept': 'application/json',
    }

    response = requests.get(full_url, auth=auth, headers=headers, data=payload)
    return response.json()


def cancel_order(auth, order_id):
    endpoint = '/cancelorder'
    full_url = api_url + endpoint  # Ensure auth.api_url is correctly defined in your auth class

    payload = {
        'order_id': order_id,
    }

    postBody = urllib.parse.urlencode(payload)
    response = requests.post(full_url, data=postBody, auth=auth,
                             headers={'Content-Type': 'application/x-www-form-urlencoded'})

    print("Request sent to:", full_url)
    print("Payload:", postBody)
    print("Response status code:", response.status_code)
    print("Response:", response.text)

    return response.json()


def edit_order(auth, size, orderId= None, limitPrice=None, stopPrice=None, clientOrderId=None):
    endpoint = '/editorder'
    full_url = api_url + endpoint

    order = {
        'size': size
    }

    if orderId is not None:
        order['orderId'] = orderId

    if clientOrderId is not None:
        order['cliOrdId'] = clientOrderId

    if stopPrice is not None:
        order['stopPrice'] = stopPrice

    if limitPrice is not None:
        order['limitPrice'] = limitPrice

    print(order)
    postBody = urllib.parse.urlencode(order)
    response = requests.post(full_url, data=postBody, auth=auth,
                             headers={'Content-Type': 'application/x-www-form-urlencoded'})

    print(response.json())
    return response.json()


"""__________________________________________________________________________________________________________________"""

# Usage
"""
order_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/sendorder')
edit_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/editorder')
fills_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/fills')
cancel_order_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/cancelorder')
open_orders_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openorders')


current_price = fetch_live_price('PF_XBTUSD')['last_price']
print(current_price)

symbol = 'PF_XBTUSD'
stop_loss = current_price * 0.996
limit_price = current_price * 0.995

order = place_order(
    order_auth, 'PF_XBTUSD', 'buy', 0.0002,
    )


stop_order = place_order(
    order_auth, 'PF_XBTUSD', 'sell', 0.0002,
    orderType='stp', stopPrice=round(stop_loss, 0), limitPrice=round(limit_price, 0))



open_orders = get_open_orders(open_orders_auth)
print(open_orders)
print('\n')

# order_id = fills['fills'][0]['order_id']

last_stop = None
order_id = None

for order in open_orders['openOrders']:
    if order['symbol'] == symbol and order['orderType'] == 'stop':
        last_stop = order
        order_id = order['order_id']
        break

print('last stop', last_stop)
print('last stop order id : ', order_id)
cancel_order(cancel_order_auth, order_id)"""

# account_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/accounts')
# account_info = get_account_info(account_auth)
# print(account_info)

# open_pos_auth = KrakenFuturesAuth(constants.kraken_public_key, constants.kraken_private_key, '/api/v3/openpositions')
# open_positions = get_open_positions(open_pos_auth)
# print(open_positions)

# instruments = get_instruments()
# print(instruments)

"""symbol = 'PI_XBTUSD'  # Change this to your specific futures symbol
price_info = fetch_live_price(symbol)
if price_info:
    print(f"Last Price for {symbol}: {price_info['last_price']}")
    print(f"Bid: {price_info['bid']}, Ask: {price_info['ask']}")
else:
    print("Price information could not be retrieved.")

historical_data = fetch_candles_since('XXBTZUSD')
print(historical_data)"""