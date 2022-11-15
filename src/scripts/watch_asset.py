from brownie import network
import time, os
from scripts.utils.utils import get_mysql_url, get_mysql_engine
from scripts.utils.aave_interfaces import get_ERC20_contract


def main(asset, freq):

    ENVIRONMENT = os.getenv('ENV')
    DATABASE = network.show_active().replace("-", "_")
    contract = get_ERC20_contract(asset)
    asset_name = contract.symbol()
    old_value = 0
    while 1:
        actual_supply = contract.totalSupply()
        if old_value != actual_supply:
            print(asset_name, actual_supply, int(time.time()))
        old_value = actual_supply
        time.sleep(freq)
        