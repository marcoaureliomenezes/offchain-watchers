import time, logging, os
from brownie import network
from scripts.utils.utils import get_kafka_producer, get_kafka_host
from scripts.utils.etherscan_api import req_etherscan, get_block_by_time_url

logging.basicConfig(level='INFO')



def main(freq, topic):
    previous_block = 0
    kafka_host = get_kafka_host()
    chain = network.show_active()
    producer = get_kafka_producer(host=kafka_host)

    while 1:
        timestamp = int(time.time())
        actual_block = req_etherscan(chain, get_block_by_time_url, {'timestamp': timestamp})
        if actual_block != previous_block:
            logging.info(actual_block)
            producer.send(topic=topic, value=actual_block)
            previous_block = actual_block
        time.sleep(float(freq))
