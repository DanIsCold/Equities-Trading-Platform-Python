import requests
import asyncio
import matplotlib
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

class marketDataHandler:
    def __init__(self):
        self.api_key = 'ZQMZITPSHXTQJZ50'
        self.base_url = 'https://www.alphavantage.co/query'
        self.file_name = 'intraday_data.txt'
        self.data = None

    def fetch_intraday_data(self, symbol, interval):
        """
        Fetches intraday market day for a given equity from Alpha Vantage.

        Parameters:

        Returns:
        """
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval' : interval,
            'apikey': self.api_key
        }
        response = requests.get(self.base_url, params=params)

        if response.status_code == 200:
            self.data = response.json()

            time_series_key = f"Time Series ({interval})"
            if time_series_key in self.data:
                with io.open(self.file_name, 'w', encoding='utf-8') as file:
                    file.write(json.dumps(self.data))
                return self.data[time_series_key]
            else:
                return {"error": "No relevant data available"}
        else:
            return {"error": f"Failed data fetch, code: {response.status_code}"}
        
        
    def load_data_from_file(self):
        try:
            with io.open(self.file_name, 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            return {"error": "File not found. Has it been fetched first?"}
        except json.JSONDecodeError:
            return {"error": "Error decoding file"}

    def proccess_data(self):
        return

    def provide_data(self):
        return
    
p1 = marketDataHandler()
loaded_data = p1.load_data_from_file()
time_series_key = "Time Series (1min)"
setup = dict(type='ohlc',volume=True, mav=(7,15,22))
if time_series_key in loaded_data:
    time_series_data = loaded_data[time_series_key]
    df = pd.DataFrame.from_dict(time_series_data, orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df.index = pd.to_datetime(df.index)

    # Sort and convert columns
    df = df.sort_index()
    df = df.astype(float)

    # Adjust time zone to US/Eastern
    df.index = df.index.tz_localize('UTC').tz_convert('US/Eastern')

    # Filter data to include only regular market hours (9:30 am to 4:00 pm)
    market_open = pd.Timestamp('09:30', tz='US/Eastern').time()
    market_close = pd.Timestamp('16:00', tz='US/Eastern').time()
    df = df.between_time(market_open, market_close)

    # Plot the closing prices
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['Close'], label='Close Price', color='blue')
    plt.title('Intraday Closing Prices')
    plt.xlabel('Time')
    plt.ylabel('Price (USD)')
    plt.legend()
    plt.grid(True)
    plt.show()

    # Plot candlestick chart with volume
    #mpf.plot(df, type='candle', volume=True, title='Intraday Price Movements', style='charles')
    mpf.plot(df,**setup)
else:
    print("Time series data not found in the file.")