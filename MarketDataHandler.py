import requests
import io
import json
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

class marketDataHandler:
    def __init__(self):
        pass

    def fetch_market_data(self, symbol, time_frame, limit, feed, currency):

        url = "https://data.alpaca.markets/v2/stocks/bars"

        params = {
            "symbols": symbol,          # The stock symbol to fetch data for
            "timeframe": time_frame,       # Timeframe for the data (e.g., 1Min, 5Min, 1Day)
            "limit": limit,             # The number of data points to fetch
            "adjustment": 'raw',       # Type of adjustment (e.g., raw, split, dividend)
            "feed": feed,             # The data feed source
            "currency": currency,         # The currency of the data
            "sort": "asc"              # Sort order (e.g., ascending or descending)
        }

        headers = {
            "Authorization": "Bearer YOUR_API_KEY"
        }

        response = requests.get(url, headers=headers, params=params)

        data = response.json()

    def market_open_check()-> bool:
        #Checks time and to see if market is open/closed, if open request current data, if closed retrieve data from DB
        return
    
