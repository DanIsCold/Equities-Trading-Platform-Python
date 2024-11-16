from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, TakeProfitRequest, StopLossRequest, TrailingStopOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from ClordidGen import clordidGenerator
import os
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "config.json")

with open(config_path) as f:
            config = json.load(f)
apikey = config['api_key']
secretkey = config['secret_key']

trading_client = TradingClient(apikey, secretkey, paper=True)
clordid = clordidGenerator().generate_order_id()

class createTrade():
    def __init__(self) -> None:
        pass

    def submit_long_order(self, symbol: str, quantity: int):
        market_order_data = MarketOrderRequest(
                    symbol,
                    quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                    )

        market_order = trading_client.submit_order(
                        order_data=market_order_data
                    )

    def clordid_long_orders(self, symbol: str, quantity: int):
        #problem submitting the request -> 404 
        market_order_data = MarketOrderRequest(
                    symbol,
                    quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    client_order_id='my_first_order',
                    )

        market_order = trading_client.submit_order(
                        order_data=market_order_data
                    )

    def submit_short_order(self, symbol: str, quantity: int):
        market_order_data = MarketOrderRequest(
                    symbol,
                    quantity,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTC
                    )

        market_order = trading_client.submit_order(
                        order_data=market_order_data
                    )
        
    def bracket_orders(self, symbol: str, quantity: int):
        bracket__order_data = MarketOrderRequest(
                    symbol,
                    quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    order_class=OrderClass.BRACKET,
                    take_profit=TakeProfitRequest(limit_price=400),
                    stop_loss=StopLossRequest(stop_price=300)
                    )

        bracket_order = trading_client.submit_order(
                        order_data=bracket__order_data
                    )

        # preparing oto order with stop loss
        oto_order_data = LimitOrderRequest(
                            symbol,
                            quantity,
                            limit_price=350,
                            side=OrderSide.BUY,
                            time_in_force=TimeInForce.DAY,
                            order_class=OrderClass.OTO,
                            stop_loss=StopLossRequest(stop_price=300)
                            )

        # Market order
        oto_order = trading_client.submit_order(
                        order_data=oto_order_data
                    )
        
    def trailing_stop_orders(self, symbol: str, quantity: int):
        trailing_percent_data = TrailingStopOrderRequest(
                    symbol,
                    quantity,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTC,
                    trail_percent=1.00 # hwm * 0.99
                    )

        trailing_percent_order = trading_client.submit_order(
                        order_data=trailing_percent_data
                    )


        trailing_price_data = TrailingStopOrderRequest(
                            symbol,
                            quantity,
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC,
                            trail_price=1.00 # hwm - $1.00
                            )

        trailing_price_order = trading_client.submit_order(
                        order_data=trailing_price_data
                    )

