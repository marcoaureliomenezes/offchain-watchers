import json, logging, os, sys
from utils.utils import run_concurrently

logging.basicConfig(level='INFO')


if __name__ == '__main__':
    parms = {k: v for k, v in [i.split("=") for i in sys.argv[1:]]}
    
    consumer_group = json.loads(os.environ['CONSUMER_GROUP'])
    list_addresses = json.loads(os.environ['LIST_ADDRESSES'])
    if type(consumer_group) == list and type(list_addresses) == list:
        if len(list_addresses) > 0 and len(consumer_group) == len(list_addresses):
            base_command = f"python contract_observer.py"
            mother_args = f"topic_in={parms['topic_in']} topic_out={parms['topic_out']}"
            child_args = lambda i: f"contract_address={list_addresses[i]} consumer_group={consumer_group[i]} process_number={i+1}"
            network = f"network={parms['network']}"
            commands = [f"{base_command} {mother_args} {child_args(i)} {network}" for i in range(len(list_addresses))]
            _  = [logging.info(f"COMMAND CHILD: {i}") for i in commands]
            commands = [i.split(" ") for i in commands]
            run_concurrently(commands)
  
