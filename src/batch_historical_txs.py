from functools import reduce
import time, os, sys, logging
import pandas as pd
import web3.eth as eth
from datetime import datetime
from utils.etherscan_api import get_txlist_url, get_block_by_time_url, req_chain_scan
from utils.utils import get_kafka_producer

pd.options.display.max_colwidth = 100
logging.basicConfig(level='INFO')

def get_block_interval(api_key, start_date, end_date=None):
    print(api_key, "API KEY")
    """METHOD get_block_interval: Receives 2 datetime objects representing a time interval
    PARM 1: API Key to use the network scan.
    PARM 2: Name of the network used.
    PARM 3: Address of the smart contract.
    PARM 4: Interval of blocks to get the transactions.
    RETURN a list of transactions interacting with the 
    """
    start_date, end_date = [int(datetime.timestamp(i)) for i in [start_date, end_date]]
    block_bottom = req_chain_scan(api_key, get_block_by_time_url, dict(timestamp=start_date, closest='after'))
    block_top = req_chain_scan(api_key, get_block_by_time_url, dict(timestamp=end_date, closest='before'))
    return int(block_bottom),int(block_top)


def produce_smart_contract_txs(producer, transaction):
    """METHOD produce_smart_contract_txs: Send a transaction to a Kafka Topic.
    PARM 1: transaction represented as a dictionary..
    PARM 2: A Kafka Producer object instanciated.
    PARM 3: The name  of the topic, in order to send the transaction message.
    RETURN a list of transactions with the fields of interest and rename these fields
    """
    topic = os.environ['TOPIC_BATCH_TX']
    try: producer.send(topic=topic, value=transaction)
    except: logging.error(f'Error sending message through topic {topic}') ; return False
    else: return True  


def get_transactions(api_key, address, startblock, endblock):
    """METHOD get_transactions:
    PARM 2: API Key to use with network scan.
    PARM 4: Block since when the module must download contract's transactions.
    PARM 5: Block till when the module must download contract's transactions.
    RETURN a list of transactions.
    """
    parms_get_txlist_url = dict(
                address=address, 
                startblock=startblock, 
                endblock=endblock, 
                offset=10000)
    list_transactions = req_chain_scan(api_key, get_txlist_url, parms_get_txlist_url)
    return list_transactions


def handle_smart_contract_txs(transactions):
    """METHOD handle_smart_contract_txs: retrieve transactions cleaned with the interest fields handled
    PARM 1: List of transactions. A list of dictionaries where each dictionary represents a transaction.
    RETURN a list of transactions with the fields of interest renamed.
    """
    interest_cols = ['blockNumber','timeStamp','from', 'to', 'isError','value','nonce','gasPrice','gasUsed','functionName', 'input']
    renamed_cols = ['block_number','timestamp','from','to','is_error','value','nonce','gasPrice','gasUsed','method', 'input']
    tx_filtered = [{col: transaction[col] for col in interest_cols} for transaction in transactions]
    tx_renamed = [{renamed_cols[col]: tx[interest_cols[col]] for col in range(len(renamed_cols))} for tx in tx_filtered]
    my_split = lambda x: x.split("(")[0]
    for i in tx_renamed:
        i['method'] = my_split(i['method'])
    return tx_renamed


def batch_contract_txs(producer, api_key, block_bottom, block_top):

    """METHOD batch_contract_txs: Downloads address transactions in batch and send it trhough a topic 
    in kafka using recursion to complete the JOB.
    PARM 1: A Kafka Producer object to use when producing data.
    PARM 2: Block since when the module must download contract's transactions.
    PARM 3: Block till when the module must download contract's transactions.
    This method is called till we reach the state whe less then 100 blocks are missing.
    """
    print(api_key, os.environ['ADDRESS_TO_SCAN'], block_bottom, block_top)
    list_gross_tx = get_transactions(api_key, os.environ['ADDRESS_TO_SCAN'], block_bottom, block_top)
    list_cleaned_tx = handle_smart_contract_txs(list_gross_tx)
    time.sleep(1)
    print(list_cleaned_tx)
    for tx in list_cleaned_tx:
        time.sleep(0.00001)
        produce_smart_contract_txs(producer, tx)

    next_bottom = max([int(i['block_number']) for i in list_cleaned_tx])
    print(block_top, next_bottom)
    print(len(list_cleaned_tx))
    if (block_top - next_bottom > 100) and (len(list_cleaned_tx) > 1):
        logging.info(f"Blocks to be analysed: {block_top - next_bottom}")
        batch_contract_txs(producer, api_key, next_bottom, block_top)
    else:
        return "COMPLETED"


if __name__ == '__main__':
    """
    MODULE: batch_historical.py
    KEY VALUE PARAMETERS:
        start_date: Date since when the module must download contract's transactions. 
        end_date: Date till when the module must download contracts's transactions.
    Get transactions made by a smart contract in a specific blockchain network.
    """
    api_keys = [os.environ[i] for i in os.environ if i[:12] == 'SCAN_API_KEY']
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    try:
        start_date = parms['start_date']
    except KeyError as e:
        logging.error(f"Parameter start_date='some date' (in format %Y-%m-%d) are required")
    else:
        end_date = parms.get('end_date')
        producer = get_kafka_producer()
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        block_bottom, block_top =  get_block_interval(api_keys[0], start_date, end_date=end_date)
        res = batch_contract_txs(producer, api_keys[0], block_bottom, block_top)

