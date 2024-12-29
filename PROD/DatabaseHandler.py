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
            

class databaseHandler:
    def __init__(self):
        self.conn = None
        self.cursor = None

    
    def connect(self):
        try:
            self.conn = psycopg2.connect(**db_config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            raise


    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    
    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            self.conn.rollback()
            raise


    def fetch_data(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise


    # inserts the fetched market data to given table
    def insert_market_data(self, symbol, market_data, table):
        try:
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
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()

        except Exception as e:
            print("An error occurred:", e)
            print("Dumping data to error_data.json for debugging...")
            # write data to a file for debugging
            with open("error_data.json", "w") as f:
                json.dump(market_data, f, indent=4)

    
    def insert_ws_data(self, data_list, table):
        try:
            if not isinstance(data_list, list):
                raise ValueError("Expected market data to be a list of dictionaries")

            insert_query = f"""
            INSERT INTO {table} (
                symbol, close_price, high_price, low_price, trade_count, open_price, time, volume, volume_weighted
            ) VALUES %s
            """

            values = [
                (
                data['S'],
                data['c'],
                data['h'],
                data['l'],
                data['n'],
                data['o'],
                data['t'],
                data['v'],
                data['vw']
            )
            for data in data_list
            ]

            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            print(f"Inserted {len(data_list)} records to the database.")
        
        except Exception as e:
            print("An error occurred:", e)
    

    def get_watchlist(self):
         #get symbols from table watchlist and return them as a list
         try:
            self.cursor.execute("SELECT symbol FROM watchlist")
            symbols = self.cursor.fetchall()
            return symbols
         
         except Exception as e:
              print("Error fetching symbols from watchlist:", e)