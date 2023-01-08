import json, sys, logging, os
from functools import lru_cache
from web3 import Web3
from utils.utils import get_kafka_producer, get_kafka_consumer
from utils.etherscan_api import req_chain_scan, get_abi_url 

logging.basicConfig(level='INFO')

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


def convert_input_transactions(producer, consumer, contract):
  for msg in consumer:
    detail_transaction = json.loads(msg.value)
    try: _, parms_method = contract.decode_function_input(detail_transaction['input'])
    except: parms_method = detail_transaction['input']
    payload = dict(tx_id=detail_transaction['tx_id'], method=detail_transaction['method'], parms=parms_method)
    try:
      producer.send(topic=topic_output, value=payload)
    except TypeError: pass


if __name__ == '__main__':
    api_key_node = os.environ['NODE_API_KEY']
    api_key_scan = os.environ['SCAN_API_KEY']
    network = os.environ["NETWORK"]
    topic_input = os.environ['TOPIC_INPUT']
    topic_output = os.environ['TOPIC_OUTPUT']
    consumer_group = os.environ['CONSUMER_GROUP']

    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    try: 
      contract_address = parms['contract_address']
      auto_offset_reset = parms['auto_offset_reset']
    except KeyError as e: logging.error(f"Parameters contract_address and auto_offset_reset are required.") 

    web3 = Web3(Web3.HTTPProvider(f'https://{network}.infura.io/v3/{api_key_node}'))
    consumer = get_kafka_consumer(topic_input, consumer_group, auto_offset_reset=auto_offset_reset)
    producer = get_kafka_producer()
    abi = req_chain_scan(api_key_scan, get_abi_url, dict(address=contract_address))
    contract, _ = _get_contract(address=contract_address, abi=abi)
    convert_input_transactions(producer, consumer, contract)

