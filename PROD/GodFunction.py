#Temp class for testing functionality between MDHandler and DBHandler

import psycopg2
from psycopg2.extras import execute_values
from MarketDataHandler import marketDataHandler
from DatabaseHandler import databaseHandler
from LiveDataHandler import liveDataHandler
from APIRateLimiter import APIRateLimiter
import aiohttp
import asyncio
import os, json, threading

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
db_config_path = os.path.join(working_directory, "db_config.json")
config_path = os.path.join(working_directory, "config.json")

with open(db_config_path) as f:
    db_config = json.load(f)

with open(config_path) as f:
    config = json.load(f)

# I JUST MOVED DBHANDLER FUNCTIONALITY TO MARKETDATAHANDLER, NEED TO REFORMAT TESTS

list = [
    "AAPL", "MSFT", "NVDA", "TSLA", "GOOG", "GOOGL", "META", "AMD", "INTC", "CRM", 
    "ORCL", "ADBE", "PYPL", "CSCO", "QCOM", "NOW", "AVGO", "IBM", "TXN", "AMAT", 
    "AMZN", "HD", "MCD", "NKE", "SBUX", "TGT", "LOW", "BKNG", "CMG", "TSCO", 
    "DLTR", "ROST", "EBAY", "MAR", "DHI", "PTON", "WMT", "COST", "TJX", "KMX", 
    "KR", "DG", "YUM", "AZO", "GPC", "BBY", "WBA", "EXPE", "ETSY", "F", "GM", 
    "HOG", "HMC", "TM", "RACE", "H", "HLT", "LVS", "WYNN", "MGM", "CCL", "NCLH", 
    "RCL", "DISH", "DIS", "NFLX", "CMCSA", "CHTR", "T", "VZ", "TMUS", "SIRI", 
    "V", "MA", "AXP", "DFS", "SYF", "COF", "C", "BAC", "JPM", "GS", "MS", "WFC", 
    "USB", "BK", "STT", "TROW", "BLK", "SCHW", "FIS", "FISV", "PAYC", "SQ", "ADP", 
    "INTU", "CRM", "CTSH", "EPAM", "ACN", "IT", "SPGI", "MSCI", "NDAQ", "CME", 
    "ICE", "MKTX", "TW", "BR", "VRSK", "MOH", "UNH", "CI", "ANTM", "HUM", "CNC", 
    "CVS", "WBA", "ABC", "MCK", "CAH", "DGX", "LH", "TDOC", "AMED", "LHCG", "PFE", 
    "JNJ", "MRK", "BMY", "ABBV", "LLY", "AMGN", "GILD", "BIIB", "REGN", "VRTX", 
    "ILMN", "ALXN", "EXEL", "NBIX", "HZNP", "MYL", "TEVA", "VTRS", "TMO", "DHR", 
    "ABT", "MDT", "SYK", "BSX", "EW", "ZBH", "ISRG", "ALGN", "GMED", "IART", "NUVA", 
    "HCA", "UHS", "THC", "ACHC", "CERN", "MDSO", "A", "WAT", "PKI", "BIO", "MTD", 
    "ZTS", "IDXX", "STE", "RMD", "HRC", "HOLX", "VAR", "BAX", "BARD", "BCR", "CRL", 
    "XRAY", "PDCO", "HSIC", "TECH", "SYNH", "PRGO", "AGN", "SHPG", "ENDP", "BHC", 
    "ENSG", "FVE", "AFAM", "BKD", "ADUS", "EHC", "PNTG", "VTR", "HCP", "LTC", "SNH", 
    "DOC", "HTA", "HR", "OHI", "NHI", "MPW", "WELL", "ARE", "DLR", "EQIX", "COR", 
    "QTS", "CONE", "IRM", "EXR", "PSA", "LSI", "NSA", "CUBE", "UMH", "ELS", "SUI", 
    "MAA", "UDR", "ESS", "EQR", "AVB", "CPT", "INVH", "AMH", "PECO", "KIM", "BRX", 
    "REG", "FRT", "AKR", "NNN", "O", "SPG", "MAC", "CBL", "WPG", "SRG", "GGP", 
    "TCO", "SKT", "JWN", "M", "KSS", "DDS", "JCPNQ", "ANF", "AEO", "GPS", "URBN", 
    "EXPR", "ROST", "TJX", "BURL", "BIG", "CHS", "LB", "SMRT", "DXLG", "SCVL", 
    "DSW", "CRI", "PLCE", "LE", "ASNA", "MYE", "LCI", "RAD", "CVS", "WBA", "ABC", 
    "CAH", "MCK", "HSIC", "PDCO", "OMI", "BDX", "BCR", "COV", "VAR", "DVA", "FMS"
]


class godFunction():
    def __init__(self) -> None:
        self.rate_limiter = APIRateLimiter(max_calls_per_minute=200)
        self.dbHandler = databaseHandler()
        self.dbHandler.connect()
        self.watchlist = self.dbHandler.get_watchlist()
    

    def md_handler_test(self):
        mdHandler = marketDataHandler(self.dbHandler)
        mdHandler.build_historical_data('AAPL', '1H', 'hourly_market_data')
    

    def ld_handler_test(self):
        ldHandler = liveDataHandler(self.dbHandler)
        try:
            threading.Thread(target=ldHandler.test_start).start()
            print('Press Ctrl+C to stop the websocket connection.')
        except KeyboardInterrupt:
            ldHandler.stop()


    async def md_threaded_calls_async(self):
        """Make asynchronous API calls for multiple symbols."""
        mdHandler = marketDataHandler('2017-11-13T00:00:00Z', '2024-11-13T21:00:00Z', 10000, 'iex', 'USD', self.rate_limiter)
        async with aiohttp.ClientSession() as session:
            tasks = [mdHandler.thread_save(symbol, '1H',session) for symbol in list]
            await asyncio.gather(*tasks)


'''
For reference, our DB tables are as follows: 
 - minute_market_data (includes live + 3 months history),
 - hourly_market_data(full history),
 - watchlist
'''
shum = godFunction()
godFunction.md_handler_test(shum) #Function to test fetching and storing historical data
#godFunction.ld_handler_test(shum) #Function to test websocket connection
#asyncio.run(shum.md_threaded_calls_async())
#Error in WebSocket connection:  module 'websocket' has no attribute 'WebSocketApp'