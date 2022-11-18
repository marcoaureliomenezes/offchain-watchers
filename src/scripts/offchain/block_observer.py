import time, logging, pika, json
from brownie import network
import pandas as pd
from scripts.utils.utils import setup_database
from scripts.utils.etherscan_api import req_etherscan, get_block_by_time_url



def create_message(connection, channel, block_no):
    channel.basic_publish(
        exchange='order',
        routing_key='order.notify',
        body = json.dumps({'block_no': block_no})
    )
    print(f'Block {block_no} broadcasted!')
    


def main(freq):
    previous_block = 0
    chain = network.show_active()
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='order', exchange_type='direct')

    while 1:

        timestamp = int(time.time())
        actual_block = req_etherscan(chain, get_block_by_time_url, {'timestamp': timestamp})
        
        if actual_block != previous_block:
            create_message(connection, channel, actual_block)
            previous_block = actual_block
        time.sleep(float(freq))
    connection.close()