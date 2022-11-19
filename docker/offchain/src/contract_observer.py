import logging, sys, json
from functools import reduce
import pandas as pd
from utils.etherscan_api import req_etherscan, get_txlist_url
from utils.utils import setup_database, get_kafka_producer, get_kafka_consumer, get_kafka_host


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


def get_transactions(network, contract_address, startblock, endblock):
    parms_get_txlist_url = dict(address=contract_address, startblock=startblock, endblock=endblock)
    list_transactions = req_etherscan(network, get_txlist_url, parms_get_txlist_url)
    return list_transactions


if __name__ == '__main__':
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    print(parms)
    logging.info(f"Process number {parms['process_number']} started")
    db_engine = setup_database(parms['network'])
    kafka_host = get_kafka_host()
    block_clock_consumer = get_kafka_consumer(kafka_host, parms['topic_in'], parms['consumer_group'])
    transactions_producer = get_kafka_producer(host=kafka_host)
    for msg in block_clock_consumer:
        block_no = json.loads(msg.value)
        list_tx = get_transactions(parms['network'], parms['contract_address'], block_no, block_no)
       
        if list_tx:
            is_tx_streamed = [stream_smart_contract_txs(tx, transactions_producer, parms['topic_out']) for tx in list_tx]
            is_tx_streamed = reduce(lambda a, b: a and b, is_tx_streamed)
            is_tx_recorded = record_smart_contract_txs(list_tx, db_engine)
            logging.info(f"Block: {block_no} Transaction streammed: {is_tx_streamed}, recorded:{is_tx_recorded}")
        else:
            logging.info(f"There's no transaction for block {block_no} in process {parms['process_number']}")