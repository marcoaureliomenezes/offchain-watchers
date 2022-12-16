from web3 import Web3

web3 = Web3(Web3.IPCProvider('~/.ethereum/geth.ipc'))

print(web3.eth.block_number)