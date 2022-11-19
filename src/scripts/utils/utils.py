import os
from subprocess import Popen
from brownie import network
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import VARCHAR, DECIMAL
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic, KafkaAdminClient
import json

def get_partition(key, all, available):
    return 0


def get_kafka_producer(host, partitioner=get_partition):
    json_serializer = lambda data: json.dumps(data).encode('utf-8')
    return KafkaProducer(bootstrap_servers=[host], value_serializer=json_serializer, partitioner=partitioner)


def get_kafka_consumer(host, topic, group_id, auto_offset_reset='latest'):
    return KafkaConsumer(topic, bootstrap_servers=host,
                        auto_offset_reset=auto_offset_reset, group_id=group_id)


def configure_kafka_topics(host, topics_config):
    admin = KafkaAdminClient(bootstrap_servers=[host])
    topics = [i for i in admin.list_topics() if i[0] != "_"]
    _ = admin.delete_topics(topics=topics) if len(topics) > 0 else None
    topic_blocks = NewTopic(name="block_clock", num_partitions=1, replication_factor=1)
    topic_txs = NewTopic(name="transactions", num_partitions=3, replication_factor=1)
    admin.create_topics(new_topics=[topic_blocks, topic_txs], validate_only=False)
    return



def get_kafka_host(env='DEV'):
    ENV = os.getenv('ENV')
    return os.getenv(f"{ENV}_KAFKA_HOST")


def get_mysql_url(database, env='DEV'):
    service, user, pwd = [os.getenv(f"{env}_{i}") for i in ('MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWD')]
    return f'mysql+pymysql://{user}:{pwd}@{service}:3306/{database}'


def get_mysql_engine(engine_url):
    db_engine = create_engine(engine_url)
    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    return db_engine


def setup_database():
    ENV = os.getenv('ENV')
    database = network.show_active().replace("-", "_")
    url_engine = get_mysql_url(database, env=ENV)
    return get_mysql_engine(url_engine)


def insert_to_database(engine_url, assets_list, table_name, mode='append'):
    db_engine = get_mysql_engine(engine_url)
    dataframe = pd.DataFrame(assets_list)
    dataframe.to_sql(table_name, con=db_engine, if_exists=mode, index=False, dtype={'price': DECIMAL(30, 0)})


def table_exists(db_engine, table_name):
    inspector = Inspector.from_engine(db_engine)
    return table_name in inspector.get_table_names()


def divide_array(array, factor):
    return [list(filter(lambda x: x % factor == i, array)) for i in range(factor)]


def remove_table_duplicated(table, db_engine):
    df = pd.read_sql(f"SELECT * FROM {table}", con=db_engine)
    df = df.drop_duplicates()
    df.to_sql(table, con=db_engine, if_exists='replace', index=False)


def run_concurrently(commands_list):
    procs = [ Popen(i) for i in commands_list ]
    for p in procs:
        p.wait()
    return


def find_holes(interval, rounds):
    df_all = pd.DataFrame([i for i in range(*interval, -1)], columns=['whole'])
    df_real = pd.DataFrame(rounds, columns=['real'])
    result = pd.merge(df_all, df_real, left_on='whole', right_on='real' ,how='left')
    result = result.loc[result['real'].isnull()].whole.values
    return result


def get_methods(object):
    return [method_name for method_name in dir(object)
                  if callable(getattr(object, method_name))]
