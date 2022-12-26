import json, requests, os
from web3 import Web3
import web3
from utils.utils import setup_database, get_kafka_producer, get_kafka_consumer
from utils.etherscan_api import req_chain_scan, get_abi_url
import traceback
import sys
from functools import lru_cache
from web3 import Web3
from web3.auto import w3
from web3.contract import Contract
from web3._utils.events import get_event_data
from web3._utils.abi import exclude_indexed_event_inputs, get_abi_input_names, get_indexed_event_inputs, normalize_event_input_types
from web3.exceptions import MismatchedABI, LogTopicError
from web3.types import ABIEvent
from eth_utils import event_abi_to_log_topic, to_hex
from hexbytes import HexBytes


@lru_cache(maxsize=None)
def _get_contract(address, abi):
  """
  This helps speed up execution of decoding across a large dataset by caching the contract object
  It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
  """
  if isinstance(abi, (str)):
    abi = json.loads(abi)

  contract = web3.eth.contract(address=Web3.toChecksumAddress(address), abi=abi)
  return (contract, abi)

if __name__ == '__main__':

    list_api_key = [os.environ[i] for i in os.environ if i[:12] == 'NODE_API_KEY']
    counter_key = 0
    key_index = list_api_key[counter_key % len(list_api_key)]
    web3 = Web3(Web3.HTTPProvider(f'https://{os.environ["NETWORK"]}.infura.io/v3/{key_index}'))

    consumer = get_kafka_consumer(os.environ['TOPIC_INPUT'], os.environ['CONSUMER_GROUP'])
    producer = get_kafka_producer()
    api_key = os.environ['SCAN_API_KEY']
for msg in consumer:
        detail_transaction = json.loads(msg.value)
        abi = req_chain_scan(api_key, get_abi_url, dict(address=detail_transaction['to']))
        contract, abi = _get_contract(address=detail_transaction['to'], abi=abi)
        func_obj, func_params = contract.decode_function_input(detail_transaction['input'])
        detail_transaction['func_obj'] = str(func_obj)
        detail_transaction['input'] = func_params
        producer.send(topic=os.environ['TOPIC_OUTPUT'], value=detail_transaction)