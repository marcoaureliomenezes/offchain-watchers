import requests, os
import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import InvalidSchema, ConnectionError

load_dotenv()

DICT_NETWORK = {
                    'mainnet': ['etherscan.io', os.getenv('ETH_API_KEY') ] , 
                    'polygon-main': ['polygonscan.com', os.getenv('POL_API_KEY')]
}


def parse_request_dataframe(request_url):
    response = requests.get(request_url)
    return pd.DataFrame(response.json()['result'])


def get_eth_balance_url(api_key, url, address):
    base_uri_method = "module=account&action=balance"
    return f"{url}?{base_uri_method}&address={address}&tag=latest&apikey={api_key}"


def get_block_by_time_url(api_key, url, timestamp, closest='before'):
    base_uri_method = "module=block&action=getblocknobytime"
    return f"{url}?{base_uri_method}&timestamp={timestamp}&closest={closest}&apikey={api_key}"


def get_txlist_url(api_key, url, address, startblock, endblock, page=1, offset=100, sort='asc'):
    base_uri_method = "module=account&action=txlist"
    return f"{url}?{base_uri_method}&address={address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={api_key}"


def get_logs_url(api_key, url, address, fromblock, toblock, page=1, offset=100):
    base_uri_method = "module=logs&action=getLogs"
    return f"{url}?{base_uri_method}&address={address}&fromBlock={fromblock}&toBlock={toblock}&page={page}&offset={offset}&apikey={api_key}" 


def req_etherscan(network, method, method_arguments):
    api_key = DICT_NETWORK[network][1]
    url = f"https://api.{DICT_NETWORK[network][0]}/api"
    request_url = method(api_key, url, **method_arguments)
    try: response = requests.get(request_url)
    except InvalidSchema as e: print(e); return
    except ConnectionError as e: print(e); return
    else:
        if response.status_code == 200:
            content = response.json()
            if content['status'] == '1':
                return content['result']
        return


if __name__ == '__main__':
    pass