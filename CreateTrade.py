from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from ClordidGen import clordidGenerator
import time

trading_client = TradingClient('PK14CV687K30ALR8RMUG', 'Fh19ACB7Kb98LwPB3Wsiu7koQCaaEA08Rt5PiXsP', paper=True)
clordid = clordidGenerator().generate_order_id()
class createTrade():
    def __init__(self) -> None:
        pass

    def submit_long_order(self):
        
        market_order_data = MarketOrderRequest(
                    symbol="AAPL",
                    qty=0.023,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=f'{clordid}',
                    )

        market_order = trading_client.submit_order(
                        order_data=market_order_data
                    )
        
        print("Order response:", market_order)

# Get our order using its Client Order ID.
my_order = trading_client.get_order_by_client_id(clordid)



joe = createTrade()
joe.submit_long_order()
time.sleep(2) 