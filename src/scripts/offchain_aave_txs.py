import time, os
import pandas as pd
from scripts.utils.etherscan_api import get_txlist, get_block_by_time
from scripts.utils.utils import setup_database

pd.options.display.max_colwidth = 100

def get_block_interval(delta_days=0):
    delta = 60*60*24*delta_days
    timestamp_top = int(time.time())
    timestamp_bottom = timestamp_top - delta
    block_top = get_block_by_time(timestamp_top, closest='before')['result']
    block_bottom = get_block_by_time(timestamp_bottom, closest='after')['result']
    return int(block_bottom),int(block_top)

def get_transactions(contract_address, startblock, endblock):
    min_txlist = lambda page, offset: get_txlist(contract_address, startblock, endblock, page, offset, sort='asc')
    df_final = pd.concat([min_txlist(i, 10000) for i in range(1)]).reset_index().drop(columns='index')
    df_final.sort_values(by=['timeStamp'], ascending=False)
    return df_final


def batch_depara_methods(df):
    df_depara_method = df[['methodId', 'functionName']].drop_duplicates()
    df_depara_method[['method_name', 'args']] = df_depara_method['functionName'].str.split('(', 1, expand=True)
    df_depara_method = df_depara_method[['methodId', 'method_name']] 
    df_depara_method.columns = ['method_id', 'method_name']
    return df_depara_method

def batch_aave_pool_transactions(df):
    old_cols = ['blockNumber', 'timeStamp', 'from', 'isError', 'value', 'nonce', 'gasPrice', 'gasUsed', 'methodId']
    new_cols = ['block_number', 'timestamp', 'client', 'is_error', 'value', 'nonce', 'gasPrice', 'gasUsed', 'method_id']
    df_tx = df[old_cols]
    df_tx.columns = new_cols
    return df_tx


def get_max_block_no(db_engine):
    query = "SELECT MAX(block_number) FROM aave_V2_transactions"
    return pd.read_sql(query, con=db_engine).values[0][0]


def refine_transactions(df):
    df_tx = batch_aave_pool_transactions(df)
    df_depara_method = batch_depara_methods(df)
    return df_tx, df_depara_method


def get_tx_data(aave_contract, db_engine, block_bottom, block_top):
    df_txs_gross = get_transactions(aave_contract, block_bottom, block_top)
    df_tx, df_depara_method = refine_transactions(df_txs_gross)
    df_tx.to_sql('aave_V2_transactions', con=db_engine, if_exists='append', index=False)
    df_depara_method.to_sql('aave_V2_depara_methods', con=db_engine, if_exists='append', index=False)
    return get_max_block_no(db_engine)

def get_all_interval(block_bottom, block_top, aave_contract, db_engine):
    next_bottom = int(get_tx_data(aave_contract, db_engine, block_bottom, block_top))
    if block_top - next_bottom > 100:
        print(block_top - next_bottom)
        get_all_interval(next_bottom, block_top, aave_contract, db_engine)
    else:
        return




def main(delta_days):
    db_engine = setup_database()
    aave_pool_contract_V2 = "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"
    block_bottom, block_top =  get_block_interval(delta_days=int(delta_days))
    res = get_all_interval(block_bottom, block_top, aave_pool_contract_V2, db_engine)
    print(res)

