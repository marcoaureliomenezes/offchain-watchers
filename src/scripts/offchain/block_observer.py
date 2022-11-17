from scripts.utils.etherscan_api import get_txlist, get_block_by_time
import pandas as pd
import time


def get_aave_pool_contract():
    pass
def get_uniswap_pools_contracts():
    pass


def get_last_block(last_block):
    actual_block = get_block_by_time(int(time.time()))
    if actual_block != last_block:
        print(actual_block)
        last_block = actual_block
        return last_block


def main():
    last_block = 0
    while 1:

        get_last_block(last_block)
        time.sleep(1)