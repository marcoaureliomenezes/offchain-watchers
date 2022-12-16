from utils.utils import create_kafka_topic


if __name__ == '__main__':

    topic_block_clock = create_kafka_topic(topic, num_partitions, replication_factor)