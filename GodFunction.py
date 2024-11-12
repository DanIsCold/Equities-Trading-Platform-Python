#Temp class for testing functionality between MDHandler and DBHandler

from MarketDataHandler import marketDataHandler
from DatabaseHandler import DatabaseHandler

class godFunction():
    def __init__(self) -> None:
        pass

    def db_dbhandler_test(self):
        dbHandler = DatabaseHandler()
        #dbHandler.connect_and_insert(limit,feed,currency)
        dbHandler.build_hourly_data('AAPL')


shum = godFunction()
shum.db_dbhandler_test()