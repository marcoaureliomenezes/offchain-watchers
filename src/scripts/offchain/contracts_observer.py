import json, logging, os
from brownie import network
import pandas as pd
from scripts.utils.etherscan_api import req_etherscan, get_txlist_url
from scripts.utils.utils import setup_database, get_kafka_producer, get_kafka_consumer, get_kafka_host, run_concurrently


logging.basicConfig(level='INFO')

def stream_smart_contract_txs(transactions, producer, topic):
    try:
        producer.send(topic=topic, value=transactions)
    except:
        logging.error('ERRO AQUI AMIGAO1')
        return False
    else: return True  

def record_smart_contract_txs(transactions, db_engine):
    try:
        df_transactions = pd.DataFrame(transactions)
        interest_cols = ['blockNumber','timeStamp','from', 'to', 'isError','value','nonce','gasPrice','gasUsed','methodId']
        renamed_cols = ['block_number','timestamp','client', 'smart_contract', 'is_error','value','nonce','gasPrice','gasUsed','method_id']
        df_transactions = df_transactions[interest_cols]
        df_transactions.columns = renamed_cols
        df_transactions.to_sql('aave_V2_tx_streaming', con=db_engine, if_exists='append', index=False)
    except: 
        logging.error('ERRO AQUI AMIGAO2')
        return False
    else: return True


def get_transactions(contract_address, startblock, endblock):
    chain = network.show_active()
    parms_get_txlist_url = dict(address=contract_address, startblock=startblock, endblock=endblock)
    list_transactions = req_etherscan(chain, get_txlist_url, parms_get_txlist_url)
    return list_transactions


def contract_observer(topic_in, topic_out, contract_address, consumer_group, process_number=0):
    logging.info(f"Process number {process_number} started")
    db_engine = setup_database()
    kafka_host = get_kafka_host()
    block_clock_consumer = get_kafka_consumer(kafka_host, topic_in, consumer_group)
    transactions_producer = get_kafka_producer(host=kafka_host)
    for msg in block_clock_consumer:
        block_no = json.loads(msg.value)
        list_tx = get_transactions(contract_address, block_no, block_no)
        if list_tx:
            is_tx_streamed = stream_smart_contract_txs(list_tx, transactions_producer, topic_out)
            is_tx_recorded = record_smart_contract_txs(list_tx, db_engine)
            logging.info(f"Block: {block_no} Transaction streammed: {is_tx_streamed}, recorded:{is_tx_recorded}")
        else:
            logging.info(f"There's no transaction for block {block_no} in process {process_number}")


def main(topic_in, topic_out):
    consumer_group = json.loads(os.environ['CONSUMER_GROUP'])
    list_addresses = json.loads(os.environ['LIST_ADDRESSES'])
    if type(consumer_group) == list and type(list_addresses) == list:
        if len(list_addresses) > 0 and len(consumer_group) == len(list_addresses):
            base_command = f"brownie run /app/scripts/offchain/contracts_observer.py contract_observer {topic_in} {topic_out}"
            suffix_command = lambda i: f"{list_addresses[i]} {consumer_group[i]} {i + 1} --network {network.show_active()}"
            commands = [f"{base_command} {suffix_command(i)}".split(" ") for i in range(len(list_addresses))]
            run_concurrently(commands)
  

