import logging, sys, os, json
from functools import reduce
from batch_historical_txs import handle_smart_contract_txs, get_transactions
from utils.utils import get_kafka_producer, get_kafka_consumer


logging.basicConfig(level='INFO')

def produce_tx(transaction, producer, topic):
    """METHOD produce_tx: Send a transaction to a Kafka Topic.
    PARM 1: transaction represented as a dictionary..
    PARM 2: A Kafka Producer object instanciated.
    PARM 3: The name  of the topic, in order to send the transaction message.
    RETURN a list of transactions with the fields of interest and rename these fields
    """
    try: producer.send(topic=topic, value=transaction)
    except: logging.error(f'Error sending message through topic {topic}') ; return False
    else: return True  


def main(api_key, contract_address, consumer_group, pid):
    """METHOD main: Consume the block_clock topic and produce for the contract_transactions topic..
    PARM 1: API Key to use with network scan.
    PARM 2: Adress of the smart contract which we pretend to follow the transactions.
    PARM 3: Name of the consumer group the will consume the block arriving from the kafka in topic.
    PARM 4: The process ID.
    RETURN a list of transactions interacting with the 
    """
    logging.info(f"Process number {pid} started")
    block_clock_consumer = get_kafka_consumer(os.environ['TOPIC_BLOCKS'], consumer_group)
    producer = get_kafka_producer()
    for msg in block_clock_consumer:
        block_no = json.loads(msg.value)['block_no']
        block_no -= 1
        list_tx = get_transactions(api_key, contract_address, block_no, block_no)
        if list_tx:
            handled_txs = handle_smart_contract_txs(list_tx)
            is_tx_streamed = [produce_tx(tx, producer, os.environ['TOPIC_STREAMING_TX']) for tx in handled_txs]
            is_tx_streamed = reduce(lambda a, b: a and b, is_tx_streamed)
            logging.info(f"Block: {block_no} streammed? {is_tx_streamed}. Number of lines: {len(handled_txs)}")
        else:
            logging.info(f"There's no transaction for block {block_no} in process {pid}")


if __name__ == '__main__':
    """
    The module streaming_addresses_txs.py acts as a child process and can be executed only by this process.
    is executed always thhrough the module streaming_addresses_txs.py.
    The father process can execute as many processes as the resources of the container allow.
    """
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    try:
        required_parms = ['api_key', 'contract_address', 'consumer_group', 'pid']
        api_key, contract_address, consumer_group, pid = [parms.get(i) for i in required_parms]
    except KeyError as e:
        logging.error(f"PARMS {required_parms} are required")
    
    main(api_key, contract_address, consumer_group, pid)