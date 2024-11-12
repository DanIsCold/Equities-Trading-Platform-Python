#Temp class for testing functionality between MDHandler and DBHandler

from MarketDataHandler import marketDataHandler
from DatabaseHandler import DatabaseHandler

class godFunction():
    def __init__(self) -> None:
        pass

    def db_insert_test(self, limit, feed, currency):
        dbHandler = DatabaseHandler('AAPL', '1Min', '2024-01-03T14:30:00Z', '2024-01-03T21:00:00Z')
        dbHandler.connect_and_insert(limit,feed,currency)

shum = godFunction()
shum.db_insert_test(1000,'iex','USD')