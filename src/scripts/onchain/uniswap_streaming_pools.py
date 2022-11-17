import time
from scripts.utils.interfaces import get_uniswap_pair_pool
from scripts.utils.utils import setup_database, table_exists
from itertools import combinations
import pandas as pd
import numpy as np


def get_pool_addresses(db_engine, table_name, pool_address):
    query = f"SELECT pool_address, pair FROM {table_name} WHERE pool_address = '{pool_address}'"
    df_pool_addresses = pd.read_sql(query, con=db_engine)
    return df_pool_addresses.values[0]


def compose_row(contract, pair):
    reserve_token_a, reserve_token_b, timestamp = contract.getReserves() 
    return {
        "pair": pair,
        "reserve_token_a": reserve_token_a, 
        "reserve_token_b": reserve_token_b,
        "total_supply": contract.totalSupply(),
        "timestamp": timestamp
    }

    
def main(pool_address):
    table_name = 'uniswap_metadata_pools'
    db_engine = setup_database()
    pool_address = get_pool_addresses(db_engine, table_name, pool_address)
    pool_contract = get_uniswap_pair_pool(pool_address[0]) 
    print(pool_address)
    while 1:

        res = compose_row(pool_contract, pool_address[1])
        print(res)

        time.sleep(1)
