import os
import json
import websocket
import threading
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "config.json")

with open(config_path) as f:
    config = json.load(f)

class LiveDataHandler:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.apikey = config['api_key']
        self.secretkey = config['secret_key']
        self.base_url = "wss://stream.data.alpaca.markets/v2/iex"
        self.testurl = "wss://stream.data.alpaca.markets/v2/test"
        self.ws = None
        self.is_running = False
    

        def on_message(self, ws, message):
            data = json.loads(message)
            print(data)
            #self.db_handler.insert_market_data(data['symbol'], data, 'minute_market_data')
        

        def on_error(self, ws, error):
            print(error)
        

        def on_close(self, ws, close_status_code, close_msg):
            print('Websocket connection closed. Code: ', close_status_code, 'Message: ', close_msg)

        
        def on_open(self, ws):
            print('Websocket connection opened.')
            self.is_running = True
            