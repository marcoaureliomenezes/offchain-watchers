import requests, os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('ETH_API_KEY')

def parse_request_dataframe(request_url):
    response = requests.get(request_url)
    return pd.DataFrame(response.json()['result'])

def get_eth_balance(address, tag='latest'):
    request_url = f"""
        https://api.etherscan.io/api
        ?module=account
        &action=balance
        &address={address}
        &tag=latest
        &apikey={API_KEY}
    """.replace('\n', '').replace(' ', '')
    return parse_request_dataframe(request_url)  

def get_block_by_time(timestamp, closest='before'):
    request_url = f"""
        https://api.etherscan.io/api
        ?module=block
        &action=getblocknobytime
        &timestamp={timestamp}
        &closest={closest}
        &apikey={API_KEY}
    """.replace('\n', '').replace(' ', '')
    response = requests.get(request_url).json()
    return response 


def get_txlist(contract_address, startblock, endblock, page=1, offset=10, sort='asc'): 
    request_url = f"""https://api.etherscan.io/api
    ?module=account
    &action=txlist
    &address={contract_address}
    &startblock={startblock}
    &endblock={endblock}
    &page={page}
    &offset={offset}
    &sort={sort}
    &apikey={API_KEY}""".replace('\n', '').replace(' ', '')
    return parse_request_dataframe(request_url)  


def get_logs_url(contract_address, fromblock, toblock, page=1, offset=10):
    request_url = f"""https://api.etherscan.io/api
    ?module=logs
    &action=getLogs
    &address={contract_address}
    &fromBlock={fromblock}
    &toBlock={toblock}
    &page={page}
    &offset={offset}
    &apikey={API_KEY}""".replace('\n', '').replace(' ', '')
    return parse_request_dataframe(request_url)  


if __name__ == '__main__':
    pass