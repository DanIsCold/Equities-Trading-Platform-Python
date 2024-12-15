import io
import json
import requests
import pytz
from datetime import datetime, timedelta, timezone
from APIRateLimiter import APIRateLimiter
import os
import asyncio

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "config.json")

with open(config_path) as f:
    config = json.load(f)
apikey = config['api_key']
secretkey = config['secret_key']


#needs to recieve a rate limiter object when rate limiter is implemented
class marketDataHandler:
    def __init__(self, db_handler):
        self.db_handler = db_handler


    def fetch_market_data(self, symbol, time_frame, start_time, end_time, limit, feed, currency):
        #Fetch market data from the Alpaca API.

        url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": time_frame,
            "start": start_time,
            "end": end_time,
            "limit": limit,
            "adjustment": 'raw',
            "feed": feed,
            "currency": currency,
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
            #self.api_call_count += 1
            if "bars" in data and symbol in data["bars"]:
                return data["bars"][symbol]
            else:
                return {"error": "No relevant data available"}
        else:
            return {"error": f"Failed data fetch, code: {response.status}"}
        
    
    def build_historical_data(self, symbol, time_frame, db_table):
        # get most recent and oldest timestamp from db table
        query = f"SELECT MIN(time), MAX(time) FROM {db_table} WHERE symbol = '{symbol}'"
        oldest_date, newest_date = self.db_handler.fetch_data(query)[0]

        # if oldest_date is None, set it to the 1st of January 2018 in the format 'YYYY-MM-DDTHH:00:00Z'
        if oldest_date is None:
            print("No data in the database, fetching data from the beginning...")
            oldest_date = datetime(2018, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
            newest_date = datetime(2018, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

        # get the closest trading timestamp to the current time in UTC
        closest_trading_timestamp = self.closest_trading_timestamp()

        print(f"Fetching data for {symbol} from {newest_date} onwards") 

        # while the most recent date in the database is less than the closest trading timestamp
        while newest_date < closest_trading_timestamp:
            # Format datetimes to strings for the API request
            oldest_date_str = oldest_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            closest_trading_timestamp_str = closest_trading_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

            # fetch market data from the API
            fetched_market_data = self.fetch_market_data(symbol, time_frame, oldest_date_str, closest_trading_timestamp_str, 10000, 'iex', 'USD')

            # insert the fetched market data to the database
            self.db_handler.insert_market_data(symbol, fetched_market_data, db_table)

            # get the most recent timestamp in the database
            query = f"SELECT MAX(time) FROM {db_table} WHERE symbol = '{symbol}'"
            db_newest_date = self.db_handler.fetch_data(query)[0]

            # ensure timestamp is timezone aware
            # THIS RETURNED A TUPLE AND NOT A DATETIME OBJECT - FIX
            if db_newest_date is not None:
                db_newest_date = db_newest_date.replace(tzinfo=pytz.utc)
            
            # if db_newest_date is the same as newest_date, break the loop
            if db_newest_date == newest_date:
                print(f"No new data fetched for {symbol}")
                break
            
            # newest_date updated for next iteration
            newest_date = db_newest_date

            # add 30 minutes to the oldest date
            oldest_date = newest_date + timedelta(minutes=30)
        
        print(f"{time_frame} data for {symbol} fetched and stored in the database")

            
    async def aysnc_fetch_market_data(self, symbol, time_frame, session):
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
        async with session.get(url, params=params, headers=headers) as response:
            if response.status == 200:
                print("API call excuted")
                data = await response.json()
                self.api_call_count += 1
                if "bars" in data and symbol in data["bars"]:
                    return data["bars"][symbol]
                else:
                    return {"error": "No relevant data available"}
            else:
                return {"error": f"Failed data fetch, code: {response.status}"}


    # Returns the closest trading timestamp to the current time in UTC in the format 'YYYY-MM-DDTHH:00:00Z'
    def closest_trading_timestamp(self):
        current_time = datetime.now(timezone.utc)

        # if current time is a weekend, return the closest trading timestamp on Friday
        if current_time.weekday() == 5:
            closest_timestamp = current_time.replace(hour=21, minute=0, second=0, microsecond=0) - timedelta(days=1)
        elif current_time.weekday() == 6:
            closest_timestamp = current_time.replace(hour=21, minute=0, second=0, microsecond=0) - timedelta(days=2)
        else:

            # define trading hours in UTC
            trading_start = current_time.replace(hour=14, minute=30, second=0, microsecond=0)
            trading_end = current_time.replace(hour=21, minute=0, second=0, microsecond=0)

            # if current time is outside trading hours, return the closest trading timestamp
            if current_time < trading_start:
                closest_timestamp = trading_start
            elif current_time > trading_end:
                closest_timestamp = trading_end
            else:
                # if current time is within trading hours, return the current time
                closest_timestamp = current_time

        # Return the closest trading timestamp in the format 'YYYY-MM-DDTHH:MM:SSZ'
        return closest_timestamp
    

    def write_market_data_to_file(self, symbol, time_frame):
        safe_end_time = self.end_time.replace(":", "_")
        #call fetch market data and write it to a file within the same directory
        market_data = self.fetch_market_data(symbol, time_frame)
        with io.open(f"{symbol}_{time_frame}_{safe_end_time}.json", 'w', encoding='utf-8') as file:
            json.dump(market_data, file, ensure_ascii=False, indent=4)

    async def thread_save(self, symbol, time_frame, session):
        market_data = await self.aysnc_fetch_market_data(symbol, time_frame, session)
        print(market_data) # used for debugging remove when fixed
        directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "threadedFiles")
        safe_end_time = self.end_time.replace(":", "_")
        file_path = os.path.join(directory, f"threaded_{symbol}_{time_frame}_{safe_end_time}.json")
        
        with io.open(file_path, 'w', encoding='utf-8') as file:
            json.dump(market_data, file, ensure_ascii=False, indent=4)

        print(f"Data saved for {symbol} at {file_path}")
