import pandas as pd
from scripts.utils.etherscan_api import req_etherscan, get_txlist_url
from scripts.utils.utils import setup_database
from scripts.utils.interfaces import get_aave_pool
import time, pika, json
from brownie import network


def configure_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    queue = channel.queue_declare('order_notify')
    queue_name = queue.method.queue
    channel.queue_bind(
        exchange = 'order',
        queue = queue_name,
        routing_key='order.notify'
    )
    return channel, queue_name


def handle_smart_contract_txs(transactions):
    if transactions:
        df_transactions = pd.DataFrame(transactions)
        interest_cols = ['blockNumber','timeStamp','from','isError','value','nonce','gasPrice','gasUsed','methodId']
        renamed_cols = ['block_number','timestamp','client','is_error','value','nonce','gasPrice','gasUsed','method_id']
        df_transactions = df_transactions[interest_cols]
        df_transactions.columns = renamed_cols
        db_engine = setup_database()
        df_transactions.to_sql('aave_V2_transactions', con=db_engine, if_exists='append', index=False)
        return df_transactions
    return "There's no transaction for this block"


def get_transactions(contract_address, startblock, endblock):
    chain = network.show_active()
    parms_get_txlist_url = dict(address=contract_address, startblock=startblock, endblock=endblock)
    list_transactions = req_etherscan(chain, get_txlist_url, parms_get_txlist_url)
    return list_transactions


def watch_new_blocks(ch, method, properties, body):
    payload = json.loads(body)
    block_no = payload['block_no']
    print(f"block {payload['block_no']} received!")
    pool_address = get_aave_pool()
    list_tx = get_transactions(pool_address, block_no, block_no)
    df_transactions = handle_smart_contract_txs(list_tx)
    print(df_transactions)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    channel, queue = configure_channel()
    channel.basic_consume(on_message_callback=watch_new_blocks, queue=queue)
    channel.start_consuming()
        
        