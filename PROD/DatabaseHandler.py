import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone, date
from MarketDataHandler import marketDataHandler
import pytz
import json
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "db_config.json")

with open(config_path) as f:
            db_config = json.load(f)
            

class DatabaseHandler():
    def __init__(self):
        pass

    
    def outside_hours(self, end_time_str: str):
        # Parse end_time from the given format and set it as UTC
        end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

        # Get the current time in UTC
        current_time = datetime.now(timezone.utc)

        # Calculate 13:30 the next day in UTC
        target_time_next_day = end_time.replace(hour=13, minute=30, second=0, microsecond=0) + timedelta(days=1)

        # Check if it's currently a weekend (Saturday or Sunday) or if the current time is within the defined hours
        if current_time.weekday() >= 5 or (end_time < current_time < target_time_next_day):
            return True
        return False


    # Fetch market data for given values from the API
    def api_fetch(self, symbol, timeframe, start_time, end_time, limit, feed, currency):
        market_data_handler = marketDataHandler(start_time, end_time, limit, feed, currency)
        market_data = market_data_handler.fetch_market_data(symbol, timeframe)
        return market_data


    # Connects to the database and inserts the fetched market data to given table
    def insert_data(self, conn, cursor, symbol, market_data, table):
        try:
            print("Inserting to the database...")

            # Insert query using execute_values for efficient bulk insertion
            insert_query = f"""
            INSERT INTO {table} (
                symbol, close_price, high_price, low_price, trade_count, open_price, time, volume, volume_weighted
            ) VALUES %s
            """

            values = [
                (
                    symbol,
                    row['c'],
                    row['h'],
                    row['l'],
                    row['n'],
                    row['o'],
                    row['t'],
                    row['v'],
                    row['vw']
                ) for row in market_data
            ]

            # Use execute_values to perform batch insertion
            execute_values(cursor, insert_query, values)
            conn.commit()
            print("Data inserted successfully.")

        except Exception as e:
            print("An error occurred:", e)

    
    # Returns the closest trading timestamp to the current time in UTC in the format 'YYYY-MM-DDTHH:00:00Z'
    def closest_trading_timestamp(self):
        current_time = datetime.now(timezone.utc)

        # if current time is a weekend, return the closest trading timestamp on Friday
        if current_time.weekday() == 5:
            closest_timestamp = current_time.replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(days=1)
        elif current_time.weekday() == 6:
            closest_timestamp = current_time.replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(days=2)
        else:

            # define trading hours in UTC
            trading_start = current_time.replace(hour=13, minute=30, second=0, microsecond=0)
            trading_end = current_time.replace(hour=20, minute=0, second=0, microsecond=0)

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
        

    # Build full hourly market data history for a given symbol
    def build_market_data(self, symbol, timeframe, db_table):
        # conn2 and cursor2 used becuase conn and cursor are used in the connect_and_insert function
        # should probably be refactored to use the same connection and cursor
        conn2 = psycopg2.connect(**db_config)
        cursor2 = conn2.cursor()
        
        # get the most recent and oldest date existing in the database table
        cursor2.execute(f"SELECT MIN(time), MAX(time) FROM {db_table} WHERE symbol = '{symbol}'")
        oldest_date, newest_date = cursor2.fetchone()

        # if oldest_date is None, set it to the 1st of January 2018 in the format 'YYYY-MM-DDTHH:00:00Z'
        if oldest_date is None:
            print("No data in the database, fetching data from the beginning...")
            oldest_date = datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            newest_date = datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
        # get the closest trading timestamp to the current time in UTC
        closest_trading_timestamp = self.closest_trading_timestamp()

        count = 0

        # while the most recent date in the database is less than the closest trading timestamp
        while newest_date < closest_trading_timestamp:
            print(f"Fetching data from {newest_date} onwards")

            # Format datetimes to strings for the API request
            oldest_date_str = oldest_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            closest_trading_timestamp_str = closest_trading_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # fetch market data from the API
            fetched_market_data = self.api_fetch(symbol, timeframe, oldest_date_str, closest_trading_timestamp_str, 10000, 'iex', 'USD')
            
            # insert the fetched market data to the database
            self.insert_data(conn2, cursor2, symbol, fetched_market_data, db_table)

            count += 1

            # get the most recent timestamp in the database
            cursor2.execute(f"SELECT MAX(time) FROM hourly_market_data WHERE symbol = '{symbol}'")
            db_newest_date = cursor2.fetchone()[0]

            # ensure timestamp is timezone aware
            if db_newest_date is not None:
                db_newest_date = db_newest_date.replace(tzinfo=timezone.utc)

            # if db_newest_date is the same as newest_date, break the loop
            if db_newest_date == newest_date:
                print("Newest timestamp unchanged, breaking loop.")
                break

            newest_date = db_newest_date

            # add 30 minutes to the oldest date
            oldest_date = newest_date + timedelta(minutes=30)
            
        
        cursor2.close()
        conn2.close()
        print(f"Hourly market data constructed in {count} API calls.")
        print("Database connection closed.")