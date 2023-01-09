import time, logging, sys, os
from requests import HTTPError
from web3 import Web3
from utils.utils import get_kafka_producer

logging.basicConfig(level='INFO')


def get_latest_block():
    try:
        block_info = web3.eth.get_block('latest')
        return block_info
    except HTTPError as e:
        if str(e)[:3] == '429': return
        return
        
if __name__ == '__main__':
    network = os.environ["NETWORK"]
    api_key_node = os.environ['NODE_API_KEY']
    clock_freq = os.environ['CLOCK_FREQUENCY']
    clock_topic = os.environ['TOPIC_CLOCK']
    tx_topic = os.environ['TOPIC_TXS']
    previous_block = 0
    producer = get_kafka_producer()
   
    while 1:
        web3 = Web3(Web3.HTTPProvider(f'https://{network}.infura.io/v3/{api_key_node}'))
        actual_block = get_latest_block()
        if not actual_block: continue
        if actual_block != previous_block:
            block_clock = {'block_no': actual_block['number'], 'timestamp': actual_block['timestamp']}
            producer.send(topic=clock_topic, value=block_clock)
            block_transactions = [bytes.hex(i) for i in actual_block['transactions']]
            _ = [producer.send(topic=tx_topic, value=i) for i in block_transactions]
            previous_block = actual_block
            logging.info(f"Data about block {block_clock['block_no']} sent!")
        time.sleep(float(clock_freq))

