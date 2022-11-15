# Chain_watcher: Capturando dados em Blockchains Públicas


## Definições importantes

### O que é Blockchain

Blockchain é um tecnologia relativamente nova e bem promissora, segundo o mercado de tecnologia em geral. Nesse ponto gostaria de separar a tecnologia em si do mercado especulativo e 

Uma rede blockchain é baseada em uma arquitetura de rede Peer-to-peer (P2P), onde cada nó da rede possui uma cópia do blockchain. Pense inicialmente na blockchain como uma lista encadeada vazia. Então o primeiro bloco é minerado.

#### Cadeia de blocos Simples (rede P2P de um nó somente)


NUM_BLOCO 0:
NONCE: Número inteiro que o minerador do bloco acertou.
DATA: Qualquer coisa.
HASH_ANTERIOR=0x00000000000000000000000000000000
HASH: 0x000000664a6ea6f46df65aef3ca1abe9

OBS: O campo HASH é a saída da função ALGORITMO_HASH(NUM_BLOCO, NONCE, DATA, HASH_ANTERIOR) < 0x000000ffffffffffffffffffffffffff


 Bloco foi mineirado quando o nó acertou um valor aleatório no campo NONCE que gerou um HASH que satisfez a condição exigida e validou os dados dentro do campo DATA. Então o segundo bloco é minerado.


NUM_BLOCO 1:
NONCE: Número inteiro que o minerador do bloco acertou.
DATA: Qualquer coisa.
HASH_ANTERIOR=0x000000664a6ea6f46df65aef3ca1abe9
HASH: 0x0000004b56ea2a379bea4aef39a1bae1

Note que o bloco contém o campo HASH_ANTERIOR. Como esse HASH_ANTERIOR é saída do algoritmo de HASH do bloco anterior, e esse algoritmo tem como entrada todos os dados deste bloco anterior, a consequencia é que enquanto cresce a cadeia de blocos, alterações nos dados de blocos que ja foram publicados anteriormente seja extremamente dificil, quando comela s ser consoderado que a rede possui múltiplos nós mineradores. Visto que o meliante precisaria minerar todos os blocos posteriores novamente em um período de tempo inversamente proporcional ao número de nós que tem na rede.


#### Cadeia de blocos distribuída (rede P2P com múltiplos nós)

Competição para ver qual nó minera um bloco.
Hashing Power.




## AAVE Um protocolo DEFI:

### Aave V2

Polygon: 
    LendingPoolAddressesProvider: 0xd05e3E715d945B59290df0ae8eF85c1BdB684744
    LendingPool: LendingPoolAddressesProvider.getLendingPool()
    PriceOracle: LendingPoolAddressesProvider.getPriceOracle()

Mainnet
    LendingPoolAddressesProvider: 0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5
    LendingPool: LendingPoolAddressesProvider.getLendingPool()
    PriceOracle: LendingPoolAddressesProvider.getPriceOracle()

Goerli:
    LendingPoolAddressesProvider: 0x5E52dEc931FFb32f609681B8438A51c675cc232d
    LendingPool: LendingPoolAddressesProvider.getLendingPool()
    PriceOracle: LendingPoolAddressesProvider.getPriceOracle()

### Aave V3

Polygon-main: 
    PoolAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb
    LendingPool: PoolAddressesProvider.getPool()
    PriceOracle: PoolAddressesProvider.getPriceOracle()  

## Uniswap: Um protocolo DEFI com funcionalidade de Exchange Descentralizada


## Uniswap V2
Ethereum-mainnet:
    UniswapV2Factory: 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f
    UniswapV2Pair = UniswapV2Factory.getPair(address1, address2)


Ethereum-goerli:
    UniswapV2Factory: 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f
    UniswapV2Pair = UniswapV2Factory.getPair(address1, address2)


### Uniswap V3

Ethereum-mainnet:
    UniswapV3Factory: 0x1F98431c8aD98523631AE4a59f267346ea31F984
    Quoter: 0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6
    UniswapV3Pair = UniswapV3Factory.getPair(address1, address2, fee) 
 
Ethereum Goerli:
    UniswapV3Factory: 0x1F98431c8aD98523631AE4a59f267346ea31F984
    Quoter: 0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6
    UniswapV3Pair = UniswapV3Factory.getPair(address1, address2, fee)

Polygon-main:
    UniswapV3Factory: 0x1F98431c8aD98523631AE4a59f267346ea31F984
    Quoter: 0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6
    UniswapV3Pair = UniswapV3Factory.getPair(address1, address2, fee) 