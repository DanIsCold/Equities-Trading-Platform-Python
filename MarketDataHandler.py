import requests

url = "https://data.alpaca.markets/v2/stocks/bars"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

print(response.text)