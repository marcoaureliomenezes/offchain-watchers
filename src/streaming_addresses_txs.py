import json, logging, os, sys
from utils.utils import run_concurrently

logging.basicConfig(level='INFO')


def compose_list_commands(list_addr, api_keys, consumer_group):
    base_comm = f"python streaming_address_txs.py"
    child_args = lambda i: f"contract_address={list_addr[i]} consumer_group={consumer_group[i]} pid={i+1}"
    dist_keys = lambda lista , i: f"api_key={lista[i % len(lista)]}"
    commands = [f"{base_comm} {child_args(i)} {dist_keys(api_keys, i)}" for i in range(len(list_addr))]
    _  = [logging.info(f"COMMAND CHILD: {i}") for i in commands]
    commands = [i.split(" ") for i in commands]
    return commands


def main():
    api_keys = [os.environ[i] for i in os.environ if i[:12] == 'SCAN_API_KEY']
    consumer_group = json.loads(os.environ['CONSUMER_GROUP'])
    list_addresses = json.loads(os.environ['LIST_ADDRESSES'])
    if type(consumer_group) != list or type(list_addresses)!= list: return
    if len(list_addresses) == 0 or len(consumer_group) != len(list_addresses): return
    commands = compose_list_commands(list_addresses, api_keys, consumer_group)
    run_concurrently(commands)


if __name__ == '__main__':
    main()


  
