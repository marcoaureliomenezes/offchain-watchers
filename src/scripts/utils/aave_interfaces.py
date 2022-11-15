import logging
from brownie import network, config, interface
from scripts.aave_datatypes import reserves_struct_v2, reserves_struct_v3
import sys

from requests import HTTPError


ACTIVE_NETWORK = config["networks"][network.show_active()]



def get_price_oracle():
    address = ACTIVE_NETWORK['aave_pool_addresses_provider']
    try:
        if network.show_active() == 'mainnet':
            pool_addresses_provider = interface.IPoolAddressesProviderV2(address)
            price_oracle_address = pool_addresses_provider.getPriceOracle()
            price_oracle = interface.IAaveOracleV2(price_oracle_address)
        else:
            pool_addresses_provider = interface.IPoolAddressesProviderV3(address)
            price_oracle_address = pool_addresses_provider.getPriceOracle()
            price_oracle = interface.IAaveOracleV3(price_oracle_address)
    except HTTPError as e:
        if str(e)[:3] == '429':
            sys.exit(13)
        print(e)
        sys.exit(14)
    return price_oracle

def get_uniswap_pools(list_tokens, address_uniswap_factory):
    pass


def get_uniswap_factory(version=2):
    print(version)
    name_uniswap_contract = f'uniswapV{version}Factory'
    try:
        address_uniswap_factory = ACTIVE_NETWORK[name_uniswap_contract]
        uniswap_contract = interface.IUniswapV2Factory(address_uniswap_factory)
    except KeyError as e: 
        print(f'{name_uniswap_contract} address not found on network!')
        sys.exit(15)
    else:
        return uniswap_contract


def get_uniswap_pair_pool(pool_address):
    return interface.IUniswapV2Pair(pool_address)




def get_V3_aggregator(oracle_contract, asset_address):
    try:
        aggregator_address = oracle_contract.getSourceOfAsset(asset_address)
        null_address = '0x0000000000000000000000000000000000000000'
        aggregator_address = aggregator_address if aggregator_address != null_address else ACTIVE_NETWORK['ether_pricefeed']
    except HTTPError as e:
        if str(e)[:3] == '429':
            sys.exit(13)
        print(e)

    return interface.AggregatorV3Interface(aggregator_address)


def get_pool():
    address = ACTIVE_NETWORK['aave_pool_addresses_provider']
    try:
        if network.show_active() == 'mainnet':
            pool_addresses_provider = interface.IPoolAddressesProviderV2(address)
            pool_address = pool_addresses_provider.getLendingPool()
            lending_pool = interface.IPoolV2(pool_address)
        else:
            pool_addresses_provider = interface.IPoolAddressesProviderV3(address)
            pool_address = pool_addresses_provider.getPool()
            lending_pool = interface.IPoolV3(pool_address)
    except HTTPError as e:
        if str(e)[:3] == '429':
            sys.exit(13)
    return lending_pool


def get_protocol_provider():
    address = ACTIVE_NETWORK['protocol_data_provider']
    aave_protocol_provider = interface.IProtocolDataeProvider(address)
    return aave_protocol_provider


def get_ERC20_contract(token_address):
    erc20_contract = interface.IERC20(token_address)
    return erc20_contract


# def get_indexes_datatypes(fields):
#     fields_list = reserves_struct_v2 if network.show_active() == 'mainnet' else reserves_struct_v3
#     get_id = lambda field: fields_list.index(list(filter(lambda x: x["campo"] == field, fields_list))[0])
#     indexes = [get_id(field) for field in fields]
#     return {fields[i]: indexes[i] for i in range(len(fields))}
    

def main(version):
    pass