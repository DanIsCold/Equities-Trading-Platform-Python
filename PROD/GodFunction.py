#Temp class for testing functionality between MDHandler and DBHandler

from MarketDataHandler import marketDataHandler
from DatabaseHandler import DatabaseHandler

class godFunction():
    def __init__(self) -> None:
        pass

    def db_handler_test(self):
        dbHandler = DatabaseHandler()
        #dbHandler.connect_and_insert(limit,feed,currency)
        dbHandler.build_market_data('IBM', '30Min', 'hourly_market_data')

    def md_handler_test(self):
        mdHandler = marketDataHandler('2017-11-13T00:00:00Z', '2024-11-13T20:00:00Z', 10000, 'iex', 'USD')
        mdHandler.write_market_data_to_file('AAPL', '1H')

#DB HANDLER CURRENTLY TRIES TO WRITE TO hour_market_data TABLE
shum = godFunction()
shum.db_handler_test()