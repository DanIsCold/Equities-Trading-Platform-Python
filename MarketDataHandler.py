import requests
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
from datetime import datetime

class marketDataHandler:
    def __init__(self):
        self.data = None


    def fetch_market_data(self, symbol, time_frame, start_time, end_time, limit, feed, currency):

        url = "https://data.alpaca.markets/v2/stocks/bars"

        params = {
            "symbols": symbol,          # The stock symbol to fetch data for
            "timeframe": time_frame,       # Timeframe for the data (e.g., 1Min, 5Min, 1Day)
            "start" : start_time,
            "end" : end_time,
            "limit": limit,             # The number of data points to fetch
            "adjustment": 'raw',       # Type of adjustment (e.g., raw, split, dividend)
            "feed": feed,             # The data feed source
            "currency": currency,         # The currency of the data
            "sort": "asc"              # Sort order (e.g., ascending or descending)
        }

        headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": "PKF881UW1TWJR2UKB6BF",
            "APCA-API-SECRET-KEY": "HaIbKS7xiU5J9fU9gbzNhnwkh248NSNSmE9nHxvy"
        }

        # Replace ':' with '-' for safe filename
        safe_end_time = end_time.replace(":", "-")

        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            self.data = response.json()

            if "bars" in self.data and symbol in self.data["bars"]:
                bars_data = self.data["bars"][symbol]
                # Write bars_data to file
                with io.open(f"{symbol}_{time_frame}_{safe_end_time}.json", 'w', encoding='utf-8') as file:
                    json.dump(self.data, file)  # Use json.dump instead of file.write
                return bars_data  # Return the actual data received
            else:
                print("Error: No relevant data available.")
                return {"error": "No relevant data available"}
        else:
            print(f"Error: Failed data fetch, code: {response.status_code}")
            return {"error": f"Failed data fetch, code: {response.status_code}"}

    def load_data_from_file(self, symbol, time_frame, end_time):
        safe_end_time = end_time.replace(":", "|")  # Ensure this is consistent
        try:
            with io.open(f"{symbol}_{time_frame}_{safe_end_time}.json", 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return {"error": "File not found. Has it been fetched first?"}
        except json.JSONDecodeError:
            return {"error": "Error decoding file"}

    def process_data(self):
        return

    def provide_data(self):
        return

# Example Usage
test1 = marketDataHandler()
bars_data = test1.fetch_market_data('AAPL', '1Min', "2024-11-01T13:30:00Z", "2024-11-01T20:00:00Z", 5000, 'iex', 'USD')
end_time = "2024-11-01T20:00:00Z"
safe_end_time = end_time.replace(":", "-")
loaded_data = test1.load_data_from_file('AAPL', '1Min', safe_end_time)

# Check if loaded_data contains error
if "error" not in loaded_data:
    aapl_data = loaded_data['bars']['AAPL']

    # Convert to DataFrame for mplfinance
    df = pd.DataFrame(aapl_data)
    df['t'] = pd.to_datetime(df['t'], utc=True)  # Convert 't' to datetime format
    df.set_index('t', inplace=True)  # Set the datetime column as the index
    df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
    df.index = df.index.tz_convert('America/New_York')

    # Plot candlestick chart
    mpf.plot(df, type='candle', style='charles', title='AAPL Candlestick Chart (ET)', volume=True)
else:
    print(loaded_data["error"])