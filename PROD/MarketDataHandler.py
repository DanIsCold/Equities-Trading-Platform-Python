import io
import json
import requests
import pytz
from datetime import datetime, time
from APIRateLimiter import APIRateLimiter
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "config.json")

with open(config_path) as f:
    config = json.load(f)
apikey = config['api_key']
secretkey = config['secret_key']


class marketDataHandler:
    def __init__(self, start_time, end_time, limit, feed, currency, rate_limiter):
        self.start_time = start_time
        self.end_time = end_time
        self.limit = limit
        self.feed = feed
        self.currency = currency
        self.api_call_count = 0
        self.rate_limiter = rate_limiter

    def fetch_market_data(self, symbol, time_frame):
        """Fetch market data from the Alpaca API."""
        if self.rate_limiter:
            self.rate_limiter.add_call()

        url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": time_frame,
            "start": self.start_time,
            "end": self.end_time,
            "limit": self.limit,
            "adjustment": 'raw',
            "feed": self.feed,
            "currency": self.currency,
            "sort": "asc"
        }
        headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": apikey,
            "APCA-API-SECRET-KEY": secretkey
        }
        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()
            self.api_call_count += 1
            if "bars" in data and symbol in data["bars"]:
                return data["bars"][symbol]
            else:
                return {"error": "No relevant data available"}
        else:
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
        #checks if markets in US are open - opening hrs 0930 - 1600
        #Honestly dont know if this even works...
        open_time = time(9,30)
        close_time = time(16,00)
        curr_uk_time = datetime.now(pytz.UTC)
        us_time = pytz.timezone("US/Eastern")
        curr_us_time = curr_uk_time.astimezone(us_time).time()

        return open_time <= curr_us_time <= close_time
    

    def write_market_data_to_file(self, symbol, time_frame):
        safe_end_time = self.end_time.replace(":", "_")
        #call fetch market data and write it to a file within the same directory
        market_data = self.fetch_market_data(symbol, time_frame)
        with io.open(f"{symbol}_{time_frame}_{safe_end_time}.json", 'w', encoding='utf-8') as file:
            json.dump(market_data, file, ensure_ascii=False, indent=4)

    def thread_save(self, symbol, time_frame):
        market_data = self.fetch_market_data(symbol, time_frame)
        directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "threadedFiles")
        safe_end_time = self.end_time.replace(":", "_")
        file_path = os.path.join(directory, f"threaded_{symbol}_{time_frame}_{safe_end_time}.json")
        
        with io.open(file_path, 'w', encoding='utf-8') as file:
            json.dump(market_data, file, ensure_ascii=False, indent=4)

        print(f"Data saved for {symbol} at {file_path}")

       

    # def threaded_request(self, symbol, time_frame):
    #     """Queue API calls and write market data to a file."""
    #     #this funky nested function is used to give the threading a callback fun
    #     def save_to_file(data):
    #         directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "threadedFiles")
    #         safe_end_time = self.end_time.replace(":", "_")
    #         file_path = os.path.join(directory, f"threaded_{symbol}_{time_frame}_{safe_end_time}.json")
            
    #         with io.open(file_path, 'w', encoding='utf-8') as file:
    #             json.dump(data, file, ensure_ascii=False, indent=4)

    #         print(f"Data saved for {symbol} at {file_path}")

    #     # Add the API call to the rate limiter queue
    #     self.rate_limiter.add_request(
    #         self.fetch_market_data,
    #         symbol,
    #         time_frame,
    #         callback=save_to_file
    #     )


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