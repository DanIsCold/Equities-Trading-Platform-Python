import requests
import asyncio
import matplotlib
import io
import json
import pandas as pd

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
if isinstance(loaded_data, dict):
    df = pd.DataFrame.from_dict(loaded_data, orient='index')
    print(df)
else:
    print("Error: Failed to load data.")