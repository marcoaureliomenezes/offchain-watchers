import pandas as pd
from scripts.utils.utils import setup_database, table_exists
from scripts.utils.aave_interfaces import get_price_oracle, get_V3_aggregator


fulfill_data_asset = lambda v3_contract, token: {
    "tokenAddress": token,
    "pair": v3_contract.description().replace(" / ", "_").lower(),
    "decimals": v3_contract.decimals(),
    "v3_aggregator_address": v3_contract.address,
    "aggregator_address": v3_contract.aggregator(),
}

get_data_asset = lambda v3_contract, token: {
    "asset": token,
    "phase_id": v3_contract.phaseId()
}

def compose_df_function(token, function):
    oracle_contract = get_price_oracle()
    v3_aggregator_contract = get_V3_aggregator(oracle_contract, token)
    try:
        return function(v3_aggregator_contract, token)
    except ValueError as e:
        return

def find_missing_oracles(df_tokens, db_engine, table):
    df_recorded = pd.read_sql_query(f"SELECT tokenAddress, phase_id FROM {table}", con=db_engine)
    df_aggregators = pd.merge(df_tokens, df_recorded, left_on=['asset', 'phase_id'], right_on=['tokenAddress', 'phase_id'], how='left')
    columns = ['asset', 'phase_id']
    filter = df_aggregators['tokenAddress'].isnull()
    token_missing_aggs = df_aggregators.loc[filter, columns]
    return token_missing_aggs


def update_metadata_oracles(tokens, table, db_engine):
    df_tokens = pd.DataFrame([compose_df_function(token, get_data_asset) for token in tokens if compose_df_function(token, get_data_asset) is not None])
    if table_exists(db_engine, table):
        df_tokens = find_missing_oracles(df_tokens, db_engine, table)
        token_missing_aggs = df_tokens['asset'].values
        if len(token_missing_aggs) == 0: return "UPDATED"
    else:
        token_missing_aggs = tokens
    df_fulfill = pd.DataFrame([compose_df_function(token, fulfill_data_asset) for token in token_missing_aggs if compose_df_function(token, fulfill_data_asset) is not None])
    df_result = pd.merge(df_tokens, df_fulfill, left_on='asset', right_on='tokenAddress', how='left')
    df_result.drop('asset', axis=1, inplace=True)
    df_result = df_result[['tokenAddress', 'pair', 'decimals', 'v3_aggregator_address', 'aggregator_address', 'phase_id']]
    df_result.to_sql(table, con=db_engine, if_exists='append', index=False)
    return "SUCCESS"


def main():

    TABLE_NAME = "metadata_oracles"
    db_engine = setup_database()
    aave_tokens = pd.read_sql_query(f"SELECT tokenAddress FROM metadata_tokens", con=db_engine)['tokenAddress'].values
    metadata_oracles = update_metadata_oracles(aave_tokens, TABLE_NAME, db_engine)
    print(metadata_oracles)