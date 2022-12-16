import logging, sys, json
from web3 import Web3
import pandas as pd
from utils.etherscan_api import req_etherscan, get_txlist_url
from utils.utils import setup_database, get_kafka_producer, get_kafka_consumer


logging.basicConfig(level='INFO')


if __name__ == '__main__':
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
  
    logging.info(f"Process number started")

    db_engine = setup_database(parms['network'])
    web3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/4dca2126970a4e6191bd6cf217e48540'))

    
    block_clock_consumer = get_kafka_consumer(parms['topic_in'], parms['consumer_group'])
    detail_transactions_producer = get_kafka_producer()
    for msg in block_clock_consumer:
        tx_id = json.loads(msg.value)
        response = web3.eth.get_transaction(tx_id)
        detail_transaction = {
            'block_number': response["blockNumber"],
            'from': response["from"],
            'to': response["to"],
            'value': response["value"],
            'gas': response["gas"],
            'gas_price': response["gasPrice"],
        }
        detail_transactions_producer.send(topic=parms['topic_out'], value=detail_transaction)

        