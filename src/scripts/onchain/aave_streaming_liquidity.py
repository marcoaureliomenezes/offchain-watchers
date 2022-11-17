from scripts.utils.utils import setup_database
from scripts.utils.interfaces import get_aave_pool
import time


def compose_row(contract, token_address):
    
    return contract.getReserveData(token_address)



def main(asset_address):
    table_name = 'uniswap_metadata_pools'
    db_engine = setup_database()
    aave_pool_contract = get_aave_pool()
    value = 36893853548754521823588
    binary = format(value, 'b')
    print(1010001055 * 61.77)
    print(1010001055 * 1245.96)
    while 1:

        res = compose_row(aave_pool_contract, asset_address)
        print(res)

        time.sleep(1)
        break
