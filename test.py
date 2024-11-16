import os
import json


with open(f'{os.getcwd()}\config.json') as f:
            config = json.load(f)
apikey = config['api_key']
secretkey = config['secret_key']

print(f'API key:{apikey}')
print(f'Secret key:{secretkey}')