import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from MarketDataHandler import marketDataHandler


db_config = {
    'dbname': 'railway',
    'user': 'postgres',
    'password': '1cbceGbfE1gBDbffGDD4EbfCf356e6gg',
    'host': 'junction.proxy.rlwy.net',
    'port': 53812
}


class DatabaseHandler():
    def __init__(self, symbol, timeframe, start_time, end_time):
        self.symbol = symbol
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time

    
    def outside_hours(self, end_time_str: str):
        # Parse end_time from the given format and set it as UTC
        end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

        # Get the current time in UTC
        current_time = datetime.now(timezone.utc)

        # Calculate 13:30 the next day in UTC
        target_time_next_day = end_time.replace(hour=13, minute=30, second=0, microsecond=0) + timedelta(days=1)

        # Check if it's currently a weekend (Saturday or Sunday)
        if current_time.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return True
        
        # Check if the current time is after end_time and before 13:30 the next day
        return end_time < current_time < target_time_next_day


    def connect_and_insert(self, limit, feed, currency):
        if self.outside_hours(self.end_time):
            print('Outside market hours, no request created!')

        else:
            market_data_handler = marketDataHandler(self.start_time, self.end_time, limit, feed, currency)
            market_data = market_data_handler.fetch_market_data(self.symbol, self.timeframe)

            try:
                conn = psycopg2.connect(**db_config)
                cursor = conn.cursor()
                print("Connected to the database.")

                try:
                    # SQL command to delete all rows in the market_data table
                    cursor.execute("DELETE FROM market_data;")
                    # Commit the transaction
                    conn.commit()
                    print("All rows in 'market_data' table deleted successfully.")
                except Exception as e:
                    print("Error deleting rows:", e)
                    # Rollback if there is an error
                    conn.rollback()

                # Insert query using execute_values for efficient bulk insertion
                insert_query = """
                INSERT INTO market_data (
                    symbol_time, symbol, close_price, high_price, low_price, trade_count, open_price, time, volume, volume_weighted
                ) VALUES %s
                """

                # Prepare data for insertion, including the new "symbol_time" column
                values = [
                    (
                        f"{self.symbol}_{datetime.strptime(row['t'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT%H:%M:%SZ')}",  # Create symbol_time
                        self.symbol,
                        row['c'],
                        row['h'],
                        row['l'],
                        row['n'],
                        row['o'],
                        #datetime.strptime(row['t'], '%Y-%m-%dT%H:%M:%SZ'),
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


