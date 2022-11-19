import time, logging, sys
from utils.utils import get_kafka_producer, get_kafka_host
from utils.etherscan_api import req_etherscan, get_block_by_time_url

logging.basicConfig(level='INFO')



if __name__ == '__main__':
    
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    previous_block = 0
    kafka_host = get_kafka_host()
    producer = get_kafka_producer(host=kafka_host)

    while 1:
        timestamp = int(time.time())
        actual_block = req_etherscan(parms['network'], get_block_by_time_url, {'timestamp': timestamp})
        if actual_block and actual_block != previous_block:
            logging.info(actual_block)
            producer.send(topic=parms['topic'], value=actual_block)
            previous_block = actual_block
        time.sleep(float(parms['freq']))
