import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone, date
from MarketDataHandler import marketDataHandler
import pytz


db_config = {
    'dbname': 'railway',
    'user': 'postgres',
    'password': '1cbceGbfE1gBDbffGDD4EbfCf356e6gg',
    'host': 'junction.proxy.rlwy.net',
    'port': 53812
}


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


    # Make API fetch request and insert data into the database
    def connect_and_insert(self, symbol, timeframe, start_time, end_time, limit, feed, currency):
        '''
        if self.outside_hours(end_time):
            print('Outside market hours, no request created!')
            return  # Exit the function if outside hours
        '''

        # Continue with data fetching and inserting if within hours
        market_data_handler = marketDataHandler(start_time, end_time, limit, feed, currency)
        market_data = market_data_handler.fetch_market_data(symbol, timeframe)

        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            print("Connected to the database.")

            '''
            try:
                # SQL command to delete all rows in the market_data table
                cursor.execute("DELETE FROM minute_market_data;")
                # Commit the transaction
                conn.commit()
                print("All rows in 'market_data' table deleted successfully.")
            except Exception as e:
                print("Error deleting rows:", e)
                # Rollback if there is an error
                conn.rollback()
            '''
                
            # Insert query using execute_values for efficient bulk insertion
            insert_query = """
            INSERT INTO hourly_market_data (
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

        finally:
            # Close the connection
            if conn:
                cursor.close()
                conn.close()
                print("Database connection closed.")
    

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
        

    
    def build_hourly_data(self, symbol):
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # get the most recent and oldest date existing in the database table
        cursor.execute(f"SELECT MIN(time), MAX(time) FROM minute_market_data WHERE symbol = '{symbol}'")
        oldest_date, newest_date = cursor.fetchone()

        # if oldest_date is None, set it to the 1st of January 2018 in the format 'YYYY-MM-DDTHH:00:00Z'
        if oldest_date is None:
            print("No data in the database, fetching data from the beginning...")
            oldest_date = '2018-01-01T00:00:00Z'
            newest_date = '2018-01-01T00:00:00Z'
    
        # get the closest trading timestamp to the current time in UTC
        closest_trading_timestamp = self.closest_trading_timestamp()

        # while the most recent date in the database is less than the closest trading timestamp
        while newest_date < closest_trading_timestamp:
            print(f"Newest data in the database: {newest_date}")
            
            # insert the next 10000 records into the database
            self.connect_and_insert(symbol, '1H', str(oldest_date), str(closest_trading_timestamp), 10000, 'iex', 'USD')

            #get the most recent and oldest date existing in the database table
            cursor.execute(f"SELECT MIN(time), MAX(time) FROM minute_market_data WHERE symbol = '{symbol}'")
            db_oldest_date, newest_date = cursor.fetchone()

            # if db_oldest_date is None, insert has failed, break the loop
            if db_oldest_date is None:
                print("Insert failed, breaking loop.")
                break
        
        cursor.close()
        conn.close()
        print("Hourly market data constructed.")
        print("Database connection closed.")