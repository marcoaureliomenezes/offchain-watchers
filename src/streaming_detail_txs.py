import logging, json, os
from web3 import Web3
from utils.utils import get_kafka_producer, get_kafka_consumer



logging.basicConfig(level='INFO')


if __name__ == '__main__':
    list_api_key = [os.environ[i] for i in os.environ if i[:12] == 'NODE_API_KEY']
    logging.info(f"Process number started")
    counter_key = 0
    key_index = list_api_key[counter_key % len(list_api_key)]
    web3 = Web3(Web3.HTTPProvider(f'https://{os.environ["NETWORK"]}.infura.io/v3/{key_index}'))

    consumer = get_kafka_consumer(os.environ['TOPIC_TXS'], os.environ['CONSUMER_GROUP'])
    producer = get_kafka_producer()
    for msg in consumer:
        tx_id = json.loads(msg.value)
        response = web3.eth.get_transaction(tx_id)
        detail_transaction = {
            'block_number': response["blockNumber"],
            'nonce': response["nonce"],
            'from': response["from"],
            'to': response["to"],
            'input': response["input"],
            'value': response["value"],
            'gas': response["gas"],
            'gas_price': response["gasPrice"],
        }
   
        producer.send(topic=os.environ['TOPIC_DETAIL_TXS'], value=detail_transaction)

        