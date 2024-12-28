import os
import json
from websocket import WebSocketApp
import time
import threading

current_dir = os.path.dirname(os.path.abspath(__file__))
working_directory = os.path.abspath(os.path.join(current_dir, os.pardir))
config_path = os.path.join(working_directory, "config.json")

with open(config_path) as f:
    config = json.load(f)

class liveDataHandler:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.apikey = config['api_key']
        self.secretkey = config['secret_key']
        self.base_url = "wss://stream.data.alpaca.markets/v2/iex"
        self.testurl = "wss://stream.data.alpaca.markets/v2/test"
        self.ws = None
        self.is_running = False
        self.stop_event = threading.Event()
    

    def on_message(self, ws, message):
        data = json.loads(message)
        if isinstance(data, list) and all('T' in entry and entry['T'] == 'b' for entry in data):
            self.db_handler.insert_ws_data(data, 'minute_market_data')
        else:
            print("Non bar message received: ", data)
        

    def on_error(self, ws, error):
        print(error)
    

    def on_close(self, ws, close_status_code, close_msg):
        print('Websocket connection closed. Code: ', close_status_code, 'Message: ', close_msg)

    
    def on_open(self, ws):
        print('Starting websocket connection...')

        auth_message = {
            "action": "auth",
            "key": self.apikey,
            "secret": self.secretkey
        }
        ws.send(json.dumps(auth_message))
        print("Authenticating...")

        subscribe_message = {
            "action": "subscribe",
            "bars": ["AAPL"]
        }
        ws.send(json.dumps(subscribe_message))
        print("Susbscription request sent:", subscribe_message)

    
    def start(self):
        self.is_running = True
        while self.is_running:
            try:
                self.ws = WebSocketApp(
                    self.base_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever()
            except Exception as e:
                print("Error in WebSocket connection: ", e)
                print("Reconnecting in 5 seconds...")
                time.sleep(5)
    

    def stop(self):
        self.is_running = False
        if self.ws:
            self.ws.close()
        print("Websocket connection closed.")


    def on_test_open(self, ws):
        print('Starting websocket connection...')

        auth_message = {
            "action": "auth",
            "key": self.apikey,
            "secret": self.secretkey
        }
        ws.send(json.dumps(auth_message))
        print("Authenticating...")

        subscribe_message = {
            "action": "subscribe",
            "bars": ["FAKEPACA"]
        }
        ws.send(json.dumps(subscribe_message))
        print("Susbscription request sent:", subscribe_message)

    
    def test_start(self):
        self.is_running = True
        while self.is_running:
            try:
                self.ws = WebSocketApp(
                    self.testurl,
                    on_open=self.on_test_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever()
            except Exception as e:
                print("Error in WebSocket connection: ", e)
                print("Reconnecting in 5 seconds...")
                time.sleep(5)
            