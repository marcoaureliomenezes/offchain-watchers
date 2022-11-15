import pandas as pd

from scripts.utils.aave_interfaces import get_ERC20_contract

def get_tokens(db_engine):
    supply_columns = ['symbol', 'tokenAddress', 'aTokenAddress', 'stableDebtTokenAddress', 'variableDebtTokenAddress']
    df_assets = pd.read_sql(f"SELECT {','.join(supply_columns)} FROM metadata_tokens", con=db_engine)
    print(df_assets.info())
    return df_assets


def compose_supply_row(tokens_info):
    #erc20_contract = get_ERC20_contract(tokens)
    result = {k: [get_ERC20_contract(i).totalSupply() for i in v] for k, v in tokens_info.items()}
    print(result)


def main():

    SCHEMA = ['pair', 'price', 'timestamp']
    url_engine = get_mysql_url(DATABASE, env=ENVIRONMENT)
    db_engine = get_mysql_engine(url_engine)
    tokens = get_tokens(db_engine)
    tokens_info = dict()
    for i in tokens.itertuples():
        tokens_info[i[1]] = i[2:6]
    
    result = compose_supply_row(tokens_info)

    #token_teste = tokens.values[0][0]
    #get_ERC20_data(token_teste)
