import logging, json, os, sys
from web3 import Web3
from utils.utils import get_kafka_producer, get_kafka_consumer

logging.basicConfig(level='INFO')

if __name__ == '__main__':
    network = os.environ["NETWORK"]
    api_key_node = os.environ['NODE_API_KEY']
    tx_topic = os.environ['TOPIC_INPUT']
    topic_output = os.environ['TOPIC_OUTPUT']
    consumer_group = os.environ['CONSUMER_GROUP']

    logging.info(f"Process number started")
    web3 = Web3(Web3.HTTPProvider(f'https://{network}.infura.io/v3/{api_key_node}'))

    consumer = get_kafka_consumer(tx_topic, consumer_group)
    producer = get_kafka_producer()
    for msg in consumer:
        tx_id = json.loads(msg.value)
        response = web3.eth.get_transaction(tx_id)
        detail_transaction = {
            'hash_id': response["hash"].hex(),
            'block_number': response["blockNumber"],
            'nonce': response["nonce"],
            'from': response["from"],
            'to': response["to"],
            'input': response["input"],
            'value': response["value"],
            'gas': response["gas"],
            'gas_price': response["gasPrice"],
        }
        producer.send(topic=topic_output, value=detail_transaction)

        