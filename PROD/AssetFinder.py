from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
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

class assetFinder():
    def __init__(self) -> None:
        pass

    def asset_list(self):
        search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
        assets = trading_client.get_all_assets(search_params)
        return assets
    
    def specific_asset_check(self, symbol: str):
        asset = trading_client.get_asset(symbol)

        if asset.tradable:
            print(f'We can trade {symbol}.')

