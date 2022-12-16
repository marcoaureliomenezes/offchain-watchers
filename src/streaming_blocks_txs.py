import time, logging, sys, os
from requests import HTTPError
from web3 import Web3
from utils.utils import get_kafka_producer


def get_latest_block():
    try:
        block_info = web3.eth.get_block('latest')
        return block_info
    except HTTPError as e:
        if str(e)[:3] == '429': return
        return
        

logging.basicConfig(level='INFO')

if __name__ == '__main__':
    """
    MODULE: streaming_blocks_txs.py
    KEY VALUE PARAMETERS:
        network: The blockchain network to get data from.
        topic_1: Kafka topic where mined blocks data will be sent.
        topic_2: Kafka topic where transactions per block data will be sent.
    Every second is tested if a new block was mined. If so, then the information about block is
    sent through the block_clock topic and transactions confirmed by this block are sent trhough 
    the transactions topic. Multipe API KEYS are allowed to be passed through environment variables.
    """
    previous_block = 0
    counter_key = 0
    producer = get_kafka_producer()
    list_api_key = [os.environ[i] for i in os.environ if i[:12] == 'NODE_API_KEY']
    while 1:
        key_index = list_api_key[counter_key % len(list_api_key)]
        web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{key_index}'))
        actual_block = get_latest_block()
        if not actual_block:
            counter_key += 1
            continue
        if actual_block != previous_block:
            block_clock = {'block_no': actual_block['number'], 'timestamp': actual_block['timestamp']}
            producer.send(topic=os.environ['TOPIC_BLOCKS'], value=block_clock)
            block_transactions = [bytes.hex(i) for i in actual_block['transactions']][:5]
            _ = [producer.send(topic=os.environ['TOPIC_TXS'], value=i) for i in block_transactions]
            previous_block = actual_block
            logging.info(f"Data about block {block_clock['block_no']} sent!")
        time.sleep(float(os.environ['CLOCK_FREQUENCY']))

