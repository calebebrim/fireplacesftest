import os
import time
from confluent_kafka.admin import (
    AdminClient,
    NewPartitions,
    NewTopic,
    ConfigResource,
    NewPartitions,
)

from confluent_kafka import (
    Producer,
    Consumer,
    KafkaException,
    KafkaError,
    admin,
    TopicPartition,
    ConsumerGroupTopicPartitions 
)


from src.services.utils.logger_utils import getLogger
from typing import Callable, Iterator

logger = getLogger(__file__)    

def create_kafka_producer(config: dict) -> Producer:
    """
    Create a Kafka Producer.
    :param config: Dict with producer config (e.g., 'bootstrap.servers').
    :return: Producer instance.
    """
    return Producer(config)


def create_kafka_consumer(config: dict, topics: list) -> Consumer:
    """
    Create a Kafka Consumer and subscribe to given topics.
    :param config: Dict with consumer config (e.g., 'bootstrap.servers', 'group.id').
    :param topics: List of topic names to subscribe.
    :return: Consumer instance.
    """
    consumer = Consumer(config)
    if topics:
        consumer.subscribe(topics)
    return consumer


def create_kafka_topic(
    config: dict, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
) -> None:
    """
    Create a Kafka topic.
    :param config: Dict with admin config (e.g., 'bootstrap.servers').
    :param topic_name: Name of the topic.
    :param num_partitions: Number of partitions.
    :param replication_factor: Replication factor.
    """
    client = admin.AdminClient(config)
    topic = admin.NewTopic(
        topic_name, num_partitions=num_partitions, replication_factor=replication_factor
    )
    logger.info(f"Creating topic '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}.")
    futures = client.create_topics([topic])

    for topic, f in futures.items():
        try:
            f.result()  # wait for operation to finish
            logger.info(f"Topic '{topic}' created.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic}': {e}")


def delete_kafka_topic(config: dict, topic_name: str) -> None:
    """
    Delete a Kafka topic.
    :param config: Dict with admin config (e.g., 'bootstrap.servers').
    :param topic_name: Name of the topic to delete.
    """
    client = admin.AdminClient(config)

    # Attempt to delete the topic
    futures = client.delete_topics([topic_name], operation_timeout=30)

    for topic, f in futures.items():
        try:
            f.result()  # Wait for the operation to complete
            logger.info(f"Topic '{topic}' deleted.")
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic}': {e}")


def list_kafka_topics(config: dict) -> list:
    """
    List existing Kafka topics.
    :param config: Dict with admin config (e.g., 'bootstrap.servers').
    :return: List of topic names.
    """
    client = admin.AdminClient(config)
    metadata = client.list_topics(timeout=10)
    return list(metadata.topics.keys())


def create_kafka_topic_if_not_exists(
    config: dict, 
    topic_name: str, 
    num_partitions: int = 1, 
    replication_factor: int = 1
) -> None:
    """
    Create the Kafka topic only if it does not already exist.
    :param config: Dict with admin config.
    :param topic_name: Topic name to check/create.
    :param num_partitions: Number of partitions.
    :param replication_factor: Replication factor.
    """
    topics = list_kafka_topics(config)
    if topic_name in topics:
        logger.warning(f"Topic '{topic_name}' already exists.")
        return

    create_kafka_topic(config, topic_name, num_partitions, replication_factor)


def create_consumer_config(offset_reset: str | None = None, consumer_group=None) -> dict:
    """
    Build consumer configuration from environment variables.
    Expected env vars:
      - KAFKA_BOOTSTRAP_SERVERS
      - KAFKA_GROUP_ID
      - KAFKA_AUTO_OFFSET_RESET (optional, default: 'earliest')
      - KAFKA_SECURITY_PROTOCOL (optional)
      - KAFKA_SASL_MECHANISM (optional)
      - KAFKA_SASL_USERNAME (optional)
      - KAFKA_SASL_PASSWORD (optional)
    """
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": consumer_group or os.getenv("KAFKA_GROUP_ID", "default-group"),
        "auto.offset.reset": offset_reset or os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    }

    # Optional security settings
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    if security_protocol:
        config["security.protocol"] = security_protocol

    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    if sasl_mechanism:
        config["sasl.mechanism"] = sasl_mechanism

    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    if sasl_username and sasl_password:
        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    return config


def create_producer_config() -> dict:
    """
    Build producer configuration from environment variables.
    Expected env vars:
      - KAFKA_BOOTSTRAP_SERVERS
      - KAFKA_SECURITY_PROTOCOL (optional)
      - KAFKA_SASL_MECHANISM (optional)
      - KAFKA_SASL_USERNAME (optional)
      - KAFKA_SASL_PASSWORD (optional)
    """
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    }

    # Optional security settings
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    if security_protocol:
        config["security.protocol"] = security_protocol

    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    if sasl_mechanism:
        config["sasl.mechanism"] = sasl_mechanism

    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    if sasl_username and sasl_password:
        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    return config

def create_admin_config() -> dict:
    """
    Build admin configuration from environment variables.
    Expected env vars:
      - KAFKA_BOOTSTRAP_SERVERS
      - KAFKA_SECURITY_PROTOCOL (optional)
      - KAFKA_SASL_MECHANISM (optional)
      - KAFKA_SASL_USERNAME (optional)
      - KAFKA_SASL_PASSWORD (optional)
    """
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    }

    # Optional security settings
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    if security_protocol:
        config["security.protocol"] = security_protocol

    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    if sasl_mechanism:
        config["sasl.mechanism"] = sasl_mechanism

    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
    if sasl_username and sasl_password:
        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    return config

def kafka_consumer_generator(consumer: Consumer, checkInterruption: Callable = lambda: False) -> Iterator:
    """
    Generator to yield messages from a Kafka consumer.
    :param consumer: Kafka Consumer instance.
    :yield: Messages from the consumer.
    """
    while True:
        if checkInterruption and checkInterruption():
            logger.debug("Interruption detected, stopping consumer.")
            break
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            yield msg
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            break


def delete_consumer_group(config: dict, group_id: str) -> None:
    """
    Delete a consumer group from Kafka.
    :param config: Dict with admin config (e.g., 'bootstrap.servers').
    :param group_id: Consumer group ID to delete.
    """
    client = AdminClient(config)

    # Attempt to delete the group
    futures = client.delete_consumer_groups([group_id])

    for group, f in futures.items():
        try:
            f.result()  # Wait for operation to complete
            logger.debug(f"Consumer group '{group}' deleted.")
        except Exception as e:
            logger.error(f"Failed to delete consumer group '{group}': {e}")

def consumer_group_topic_partitions_to_dict(group_partitions_list: list[ConsumerGroupTopicPartitions]) -> dict:
    """
    Convert a list of ConsumerGroupTopicPartitions to a dictionary.
    :param group_partitions_list: List of ConsumerGroupTopicPartitions.
    :return: Dictionary with group_id as key and list of TopicPartition as value.
    """
    result = {}
    for group_partitions in group_partitions_list:
        if group_partitions.topic_partitions:
            result[group_partitions.group_id] = [
                {
                "topic": tp.topic,
                "partition": tp.partition,
                "offset": tp.offset,
                }
                for tp in group_partitions.topic_partitions
            ]
    return result

def reset_consumer_group_to_earliest(group_id: str, topic: str):
    """
    Reset the offsets of a consumer group to the earliest offsets for a specific topic.
    :param group_id: Consumer group ID to reset.
    :param topic: Topic for which to reset the offsets."""
    logger.info(f"Resetting consumer group '{group_id}' to earliest offsets for topic '{topic}'...")
    admin_client = AdminClient(create_admin_config())

    consumer: Consumer = Consumer(create_consumer_config(offset_reset='earliest', consumer_group=group_id))

    # Retrieve partition information for the topic
    metadata = consumer.list_topics(topic=topic, timeout=10)
    if topic not in metadata.topics:
        logger.error(f"Topic '{topic}' not found.")
        consumer.close()
        return
    logger.debug(f"Found topic '{topic}' with partitions: {metadata.topics[topic].partitions.keys()}")

    partitions = metadata.topics[topic].partitions.keys()
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    # Fetch the earliest offsets for each partition
    earliest_offsets = {}
    for tp in topic_partitions:
        low, _ = consumer.get_watermark_offsets(tp, timeout=10)
        tp.offset = low  # Set the offset to the earliest available

    consumer_group_partitions = ConsumerGroupTopicPartitions(group_id=group_id, topic_partitions=topic_partitions)
    consumer.close()

    # Alter the consumer group offsets
    futures = admin_client.alter_consumer_group_offsets([consumer_group_partitions])
    # Wait for each operation to finish
    for tp, future in futures.items():
        try:
            future.result()
            logger.info(f"Successfully reset offset for {tp}")
        except Exception as e:
            logger.error(f"Failed to reset offset for {tp}: {e}")

    logger.info(f"Consumer group '{group_id}' has been reset to the earliest offsets for topic '{topic}'.")


def get_consumer_group_lag(group_id: str, topic: str) -> dict:
    """
    Calculate the lag for a consumer group on a specific topic.

    :param bootstrap_servers: Comma-separated list of Kafka broker addresses.
    :param group_id: The consumer group ID.
    :param topic: The topic to check lag for.
    :return: Dictionary {partition: lag}.
    """
    admin = AdminClient(create_admin_config())

    # Fetch topic metadata to get all partitions
    topic_metadata = admin.list_topics(topic=topic, timeout=10)
    partitions = list(topic_metadata.topics[topic].partitions.keys())

    # Prepare TopicPartition list for each partition
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    consumer_group_topic_partitions = ConsumerGroupTopicPartitions(
        group_id, topic_partitions
    )

    # Fetch committed offsets for the consumer group
    committed = admin.list_consumer_group_offsets([consumer_group_topic_partitions])

    # Create a temporary consumer to fetch end offsets (latest)
    from confluent_kafka import Consumer

    consumer = Consumer(create_consumer_config(consumer_group=group_id))

    lag = {}
    for tp in topic_partitions:
        committed_offset = committed.get(tp, None)
        committed_offset = (
            committed_offset.offset if committed_offset is not None else 0
        )

        low, high = consumer.get_watermark_offsets(tp, timeout=5)
        lag[tp.partition] = max(high - committed_offset, 0)

    consumer.close()
    return lag
