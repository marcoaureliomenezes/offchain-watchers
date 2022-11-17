from brownie import network
import pandas as pd
from requests import HTTPError
from scripts.utils.utils import setup_database, table_exists
from scripts.utils.interfaces import get_price_oracle
import time, os, sys
from datetime import datetime

def get_tokens(db_engine):
    df_assets = pd.read_sql("SELECT pair, MAX(tokenAddress) FROM metadata_oracles GROUP BY pair", con=db_engine)
    return df_assets.values

def read_last_record(db_engine, table, threshold):
    if table_exists(db_engine, table):
        query1 = f"SELECT * FROM price_streaming ORDER BY timestamp DESC LIMIT {20 * threshold}"
        query2 = f"""
        SELECT c.pair, MAX(c.timestamp) AS timestamp FROM
                (SELECT * FROM price_streaming ORDER BY timestamp DESC LIMIT {20 * threshold}) c
                GROUP BY c.pair
            """
        df1 = pd.read_sql(query1, con=db_engine)
        df2 = pd.read_sql(query2, con=db_engine)
        df_assets = pd.merge(df1, df2, on=['pair', 'timestamp'])
    else:
        df_assets = pd.DataFrame([], columns=['pair', 'price', 'timestamp'])
    return df_assets.astype({'timestamp': 'int64', 'price': 'object'})


def get_new_frame(oracle_contract, pair_address, schema):
    timestamp = int(time.time())
    try:
        assets_new_prices = oracle_contract.getAssetsPrices([i[1] for i in pair_address])
    except HTTPError as e:
        if str(e)[:3] == '429':
            sys.exit(13)
    new_prices = [(pair_address[i][0], assets_new_prices[i], timestamp) for i in range(len(assets_new_prices))]
    df_actual_prices = pd.DataFrame(new_prices, columns=schema).astype({'timestamp': 'int64', 'price': 'object'})
    return df_actual_prices.sort_values(['pair', 'timestamp'], ascending=True)

def secure_write_sql(df_new_record, table_name, db_engine, threshold):
    df_last_record = read_last_record(db_engine, 'price_streaming', threshold)
    df_result = pd.merge(df_new_record, df_last_record, on=['pair', 'price'], how='left')
    df_result = df_result.loc[df_result['timestamp_y'].isnull()].drop(columns=['timestamp_y'])
    df_result.columns = ['pair', 'price', 'timestamp']
    df_result.to_sql(table_name, con=db_engine, index=False, if_exists='append')

def main():
    SCHEMA = ['pair', 'price', 'timestamp']
    db_engine = setup_database()
    pair_address = get_tokens(db_engine)
    threshold = len(pair_address)
    oracle_contract = get_price_oracle()
    df_record = pd.DataFrame([], columns=SCHEMA)

    while 1:
        print(f"AGORA: {datetime.now()}")
        df_actual_prices = get_new_frame(oracle_contract, pair_address, SCHEMA)
        df_record = pd.concat([df_record, df_actual_prices])
        if df_record.shape[0] > 10 * threshold:
            df_record = df_record.groupby(['pair', 'price']).agg({'timestamp': 'min'}).reset_index()
            if df_record.shape[0] > 1.5 * threshold:
                secure_write_sql(df_record, 'price_streaming', db_engine, threshold) 
                df_record = pd.DataFrame([], columns=SCHEMA)
                continue
        time.sleep(1.2)


