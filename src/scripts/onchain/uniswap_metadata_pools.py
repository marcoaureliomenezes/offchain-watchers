import sys
from brownie import interface
from scripts.utils.interfaces import get_uniswap_factory, get_uniswap_pair_pool
from scripts.utils.utils import setup_database, table_exists
from itertools import combinations
import pandas as pd
import numpy as np


NULL_ADDRESS = '0x0000000000000000000000000000000000000000'


def get_pair_pure_combinations(tokens):
    list_pool_pairs =list(combinations(tokens, 2))
    return [tuple(sorted(i)) for i in list_pool_pairs]


def what_to_append(pairs, table_name, db_engine):
    if table_exists(db_engine, table_name):
        df_pairs = pd.DataFrame(pairs ,columns=['address_token_a', 'address_token_b'])
        query = f"SELECT address_token_a, address_token_b FROM uniswap_metadata_pools WHERE pool_address <> {NULL_ADDRESS};"
        df_pairs_recorded = pd.read_sql(query, con=db_engine)
        df_pairs_recorded['recorded'] = 'S'
        df_merged = pd.merge(df_pairs, df_pairs_recorded, on=['address_token_a', 'address_token_b'], how='left')
        df_merged = df_merged[df_merged['recorded'].isnull()]
        df_missing_pair_pools = df_merged[['address_token_a', 'address_token_b']].values
        list_missing_pair_pools = [(i, j) for i, j in df_missing_pair_pools]
  
        return list_missing_pair_pools
    return pairs




def get_metadata_tokens(db_engine):
    query = "SELECT tokenAddress, symbol FROM metadata_tokens;"
    df_tokens = pd.read_sql(query, con=db_engine)
    return df_tokens


def efficient_append(df, table_name, db_engine):
    return df.to_sql(table_name, con=db_engine, if_exists='append', index=False)


def handle_token_symbols(df):
    df['identifier'] = np.where(df['symbol'] != 'Undefined', df['symbol'], df['tokenAddress'])
    df = df.drop(columns=['symbol'])
    df.columns = ['tokenAddress', 'pair']
    return df


def get_metadata_pools(uniswap_factory, token_pairs):
    list_pool_addresses = [(uniswap_factory.getPair(tokenA, tokenB), tokenA, tokenB) for tokenA, tokenB in token_pairs]
    addresses_pools = [contract for contract, _, _ in list_pool_addresses]
    list_pool_contracts = [get_uniswap_pair_pool(contract) if contract != NULL_ADDRESS else None for contract in addresses_pools]

    list_pool_metadata = [(contract.symbol(), contract.decimals()) 
                                        if contract else ( None, None) for contract in list_pool_contracts]
    
    data = [list_pool_addresses[i] + list_pool_metadata[i] for i in range(len(list_pool_addresses))]
    uniswap_pool_metadata_columns = ['pool_address', 'address_token_a', 'address_token_b', 'symbol', 'decimals']
    df = pd.DataFrame(data ,columns=uniswap_pool_metadata_columns).astype({'decimals': 'Int64'})
    return df

def get_pair_name(df_pools, df_metadata_tokens):
    df_pools = pd.merge(df_pools, df_metadata_tokens, left_on='address_token_a', right_on='tokenAddress')
    df_pools = df_pools.drop(columns=['tokenAddress'])
    df_pools = pd.merge(df_pools, df_metadata_tokens, left_on='address_token_b', right_on='tokenAddress')
    df_pools = df_pools.drop(columns=['tokenAddress'])
    df_pools['pair'] = df_pools[['pair_x', 'pair_y']].apply(lambda x: '_'.join(x), axis=1)
    df_pools = df_pools.drop(columns=['pair_x', 'pair_y'])
    return df_pools

def method(uniswap_factory, db_engine, table_name):
    df_metadata_tokens = get_metadata_tokens(db_engine)
    df_metadata_tokens = handle_token_symbols(df_metadata_tokens)    
    list_tokens = df_metadata_tokens['tokenAddress'].values
    sorted_list_pool_pairs = get_pair_pure_combinations(list_tokens)
    sorted_filtered_list_pool_pairs = what_to_append(sorted_list_pool_pairs, table_name, db_engine)
    if len(sorted_filtered_list_pool_pairs) == 0: return "UPDATED"
    df_pools = get_metadata_pools(uniswap_factory, sorted_filtered_list_pool_pairs)
    df_pools = get_pair_name(df_pools, df_metadata_tokens)
    efficient_append(df_pools, table_name, db_engine)
    return "SUCCESS"


def main(version):
    table_name = 'uniswap_metadata_pools'
    db_engine = setup_database()
    uniswap_factory = get_uniswap_factory(version)
    update_table_uniswap_pools = method(uniswap_factory, db_engine, table_name)
    print(update_table_uniswap_pools)
    
