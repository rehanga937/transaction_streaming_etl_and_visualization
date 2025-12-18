from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from src import config


def create_topic(topic_name: str, num_partitions: int, replication_factor: int):
    """Create a topic in the Kafka cluster.

    Prints result to terminal.

    Args:
        topic_name (str): _description_
        num_partitions (int): _description_
        replication_factor (int): _description_
    """
    admin_client = AdminClient({"bootstrap.servers": config.KAKFA_BROKERS_FOR_LOCAL_MACHINE})

    metadata = admin_client.list_topics()
    if topic_name in metadata.topics:
        print(f"\033[94mTopic {topic_name} already exists.\033[00m")
        return
    
    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        future = admin_client.create_topics([new_topic])[topic_name]
        future.result()
        print(f"\033[92mTopic {topic_name} created.\033[00m")
    except Exception as e:
        print(f"\033[91mTopic {topic_name} creation failed. Error: {type(e)}: {e}\033[00m")