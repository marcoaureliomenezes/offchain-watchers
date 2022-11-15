
from brownie import network, interface
import pandas as pd
from requests import HTTPError
from scripts.utils.utils import get_mysql_url, get_mysql_engine, table_exists
from scripts.utils.aave_interfaces import get_pool, get_indexes_datatypes
from dotenv import load_dotenv
import os, sys

load_dotenv('/app/.env')

def get_reserve_tokens(token):
    pool_contract = get_pool()
    list_type_tokens = ["aTokenAddress", "stableDebtTokenAddress", "variableDebtTokenAddress"]
    res = get_indexes_datatypes(list_type_tokens)
    tokens = [pool_contract.getReserveData(token)[res[type_token]] for type_token in list_type_tokens]
    return {"tokenAddress": token, **{list_type_tokens[i]: tokens[i] for i in range(len(tokens))}}


def get_ERC20_metadata(token, **kwargs):
    ERC20_contract = interface.IERC20(token)
    res = dict(tokenAddress = token, decimals = ERC20_contract.decimals())
    try:
        res['name'] = ERC20_contract.name()
        res['symbol'] = ERC20_contract.symbol()

    except OverflowError as e:
        print(f"Error with token {token}")
        res['name'] = None
        res['symbol']  = None
    return res


def find_missing_tokens(all_tokens, db_engine, table):
    df_all_tokens = pd.DataFrame(all_tokens, columns=['token_address'])
    df_recorded_tokens = pd.read_sql_query(f"SELECT tokenAddress FROM {table}", con=db_engine)
    df_tokens = pd.merge(df_all_tokens, df_recorded_tokens, left_on='token_address', right_on='tokenAddress', how='left')
    token_missing = df_tokens.loc[df_tokens['tokenAddress'].isnull(), 'token_address'].values
    return token_missing


def update_metadata_tokens(tokens, table, db_engine):  
    if table_exists(db_engine, table):
        missing_tokens = find_missing_tokens(all_tokens=tokens, db_engine=db_engine, table=table)
        if len(missing_tokens) == 0: return "UPDATED"
    else:
        missing_tokens = tokens
    try:
        df_erc20_data = pd.DataFrame([get_ERC20_metadata(token) for token in missing_tokens])
        df_reserve_data = pd.DataFrame([get_reserve_tokens(token) for token in missing_tokens])
    except HTTPError as e:
        print(e.response)
        sys.exit(13)
    df_token_metadata = pd.merge(df_erc20_data, df_reserve_data, on="tokenAddress", how="left")
    df_token_metadata.to_sql(table, con=db_engine, if_exists='append', index=False)
    return 'SUCCESS'
 

def main():

    TABLE_NAME, ENVIRONMENT = ("metadata_tokens", os.getenv('ENV'))
    database = network.show_active().replace("-", "_")
    url_engine = get_mysql_url(database, env=ENVIRONMENT)
    db_engine = get_mysql_engine(url_engine)
    aave_tokens = get_pool().getReservesList()
    metadata_assets = update_metadata_tokens(aave_tokens, TABLE_NAME, db_engine)
    print(metadata_assets)




