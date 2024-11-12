import requests
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
from datetime import datetime, time
import pytz 

class marketDataHandler:
    def __init__(self, start_time, end_time, limit, feed, currency):
        self.start_time = start_time
        self.end_time = end_time
        self.limit = limit
        self.feed = feed
        self.currency = currency


    def fetch_market_data(self, symbol, time_frame):
        #check if market open, if open run this, if not pull data from db
        url = "https://data.alpaca.markets/v2/stocks/bars"

        params = {
            "symbols": symbol,          # The stock symbol to fetch data for
            "timeframe": time_frame,       # Timeframe for the data (e.g., 1Min, 5Min, 1Day)
            "start" : self.start_time,
            "end" : self.end_time,
            "limit": self.limit,             # The number of data points to fetch
            "adjustment": 'raw',       # Type of adjustment (e.g., raw, split, dividend)
            "feed": self.feed,             # The data feed source
            "currency": self.currency,         # The currency of the data
            "sort": "asc"              # Sort order (e.g., ascending or descending)
        }

        headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": "PK14CV687K30ALR8RMUG",
            "APCA-API-SECRET-KEY": "Fh19ACB7Kb98LwPB3Wsiu7koQCaaEA08Rt5PiXsP"
        }

        response = requests.get(url, params=params, headers=headers)

        #print(response.text[:500])

        if response.status_code == 200:
            self.data = response.json()

            if "bars" in self.data and symbol in self.data["bars"]:
                bars_data = self.data["bars"][symbol]
                return bars_data  # Return the actual data received
            else:
                print("Error: No relevant data available.")
                return {"error": "No relevant data available"}
        else:
            print(f"Error: Failed data fetch, code: {response.status_code}")
            return {"error": f"Failed data fetch, code: {response.status_code}"}


#This function now will get called if market is closed. So instead of reading from a 
#file it has to check the database for the info 
    def load_data_from_file(self, symbol, time_frame):
        safe_end_time = self.end_time.replace(":", "|")  # Ensure this is consistent
        try:
            with io.open(f"{symbol}_{time_frame}_{safe_end_time}.json", 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return {"error": "File not found. Has it been fetched first?"}
        except json.JSONDecodeError:
            return {"error": "Error decoding file"}


    def check_market_open(self):
        #checks if markets in US are open - opening hrs 0930 - 1630
        #Honestly dont know if this even works...
        open_time = time(9,30)
        close_time = time(16,30)
        curr_uk_time = datetime.now(pytz.UTC)
        us_time = pytz.timezone("US/Eastern")
        curr_us_time = curr_uk_time.astimezone(us_time).time()

        return open_time <= curr_us_time <= close_time

'''
    def plot_candle(self, symbol, time_frame):
        
        end_time = "2024-11-01T20:00:00Z"
        safe_end_time = end_time.replace(":", "-")
        loaded_data = self.load_data_from_file(symbol, time_frame, safe_end_time)

        if "error" not in loaded_data:
            aapl_data = loaded_data['bars']['AAPL']

            # Convert to DataFrame for mplfinance
            df = pd.DataFrame(aapl_data)
            df['t'] = pd.to_datetime(df['t'], utc=True)  # Convert 't' to datetime format
            df.set_index('t', inplace=True)  # Set the datetime column as the index
            df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
            df.index = df.index.tz_convert('America/New_York')

            # Plot candlestick chart
            mpf.plot(df, type='candle', style='charles', title=f'{symbol} Candlestick Chart (ET)', volume=True)
        else:
            print(loaded_data["error"])
        return
'''