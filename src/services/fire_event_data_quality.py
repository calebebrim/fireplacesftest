import os
import time
import json
from datetime import datetime

from src.services.utils.dateutils import try_strptime, try_strftime
from src.services.utils.logger_utils import getLogger, hline
from src.services.models.fire_event import (
    FireEvent,
    data_quality_analysis,
    parse_fire_event,
)
from src.services.utils.kafka_utils import (
    create_consumer_config,
    create_producer_config,
    delete_kafka_topic,
    create_kafka_topic_if_not_exists,
    create_kafka_producer,
    create_kafka_consumer,
    kafka_consumer_generator,
    reset_consumer_group_to_earliest,
    get_consumer_group_lag,
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
DATE_FORMAT = os.getenv("DATE_FORMAT", "%Y-%m-%dT%H:%M:%S.%fZ")
DATETIME_FORMAT = os.getenv("DATETIME_FORMAT", "%Y-%m-%dT%H:%M:%S.%fZ").split("|")

EFFECTIVE_DATETIME_FORMATS = DATETIME_FORMAT + [DATE_FORMAT]

ON_FAILURE = os.getenv("ON_FAILURE", "continue").lower()

EVENTS_SOURCE_TOPIC = os.getenv("EVENTS_SOURCE_TOPIC", "fire_event_source")
EVENTS_SOURCE_TOPIC_CG = os.getenv("EVENTS_SOURCE_TOPIC_CG", None)

VALIDATED_EVENTS_TOPIC = os.getenv("VALIDATED_EVENTS_TOPIC", "validated-fire-events")
VALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR = int(
    os.getenv("VALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR", 1)
)
VALIDATED_EVENTS_TOPIC_PARTITIONS = int(
    os.getenv("VALIDATED_EVENTS_TOPIC_PARTITIONS", 30)
)

UNVALIDATED_EVENTS_TOPIC = os.getenv(
    "UNVALIDATED_EVENTS_TOPIC", "validation-failed-fire-events"
)
UNVALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR = int(
    os.getenv("UNVALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR", 1)
)
UNVALIDATED_EVENTS_TOPIC_PARTITIONS = int(
    os.getenv("UNVALIDATED_EVENTS_TOPIC_PARTITIONS", 1)
)

RESTART = os.getenv("RESTART", "false").lower() == "true"
MAIN_LOOP = os.environ.get("MAIN_LOOP", "True").lower() == "true"
MAIN_LOOP_INTERVAL = int(os.environ.get("MAIN_LOOP_INTERVAL", 30))
MAIN_LOOP_TIMEOUT = int(os.environ.get("MAIN_LOOP_TIMEOUT", 60))

SERVICE_NAME = os.environ.get("SERVICE_NAME", "fire_event_data_quality_service")

FLUSH_TIMEOUT = os.environ.get("FLUSH_TIMEOUT", 3)


def main():
    logger = getLogger(__file__)
    effective_consumer_group = EVENTS_SOURCE_TOPIC_CG or SERVICE_NAME
    consumer_config = create_consumer_config(
        consumer_group=effective_consumer_group
    )
    producer_config = create_producer_config()

    producer = create_kafka_producer(producer_config)
    consumer = create_kafka_consumer(consumer_config, [EVENTS_SOURCE_TOPIC])
    lag = get_consumer_group_lag(effective_consumer_group, EVENTS_SOURCE_TOPIC)

    if RESTART:
        logger.info("Restarting Fire Event Data Quality Resources...")
        delete_kafka_topic(
            config=producer_config, topic_name=VALIDATED_EVENTS_TOPIC
        )
        delete_kafka_topic(
            config=producer_config, topic_name=UNVALIDATED_EVENTS_TOPIC
        )
        # start over from the earliest messages
        reset_consumer_group_to_earliest(topic=EVENTS_SOURCE_TOPIC, group_id=effective_consumer_group)
        logger.info("Fire Event Data Quality Resources restarted.")
        hline()
        logger.info("<RESTARTED>")
        hline()
        return

    create_kafka_topic_if_not_exists(
        config=producer_config,
        topic_name=VALIDATED_EVENTS_TOPIC,
        num_partitions=VALIDATED_EVENTS_TOPIC_PARTITIONS,
        replication_factor=VALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR,
    )
    create_kafka_topic_if_not_exists(
        config=producer_config,
        topic_name=UNVALIDATED_EVENTS_TOPIC,
        num_partitions=UNVALIDATED_EVENTS_TOPIC_PARTITIONS,
        replication_factor=UNVALIDATED_EVENTS_TOPIC_REPLICATION_FACTOR,
    )

    time.sleep(5)  # wait for topics to be created

    logger.info("Fire Event Data Quality Service is running...")
    hline(as_debug=True)
    logger.debug(f"{effective_consumer_group} lag for {EVENTS_SOURCE_TOPIC}: {lag}")
    hline(as_debug=True)

    stopped = False
    generator = None
    while True:
        processed_messages = 0  
        messages_with_errors = 0
        sucessful_messages = 0
        latest_successful_event = None
        start_time = time.time()

        def stop():
            end_time = time.time()
            elapsed_time = end_time - start_time
            return processed_messages >= BATCH_SIZE or elapsed_time > MAIN_LOOP_TIMEOUT

        try:
            if not generator:
                generator = kafka_consumer_generator(consumer)
                logger.debug("Consumer is waiting for assignment.")
            for message in generator:
                if stop():
                    break
                if message is None:
                    continue
                message_key = message.key().decode("utf-8") if message.key() else None
                message_value = message.value().decode("utf-8")

                hline(header=str(message_key), char="=", as_debug=True)
                logger.debug(f"Received message {message_key}...")

                try:
                    logger.debug(f"Decoding message {message_key}...")
                    event_dict = json.loads(message_value)
                except json.JSONDecodeError as e:
                    messages_with_errors += 1
                    logger.error(f"Failed to decode message {message_key}: {e}")
                    logger.error(f"Failed {message_key} body: {message_value}")
                    if ON_FAILURE == "continue":
                        logger.error(f"Failed to decode message {message_key}: {e}")
                        continue
                    elif ON_FAILURE == "raise":
                        raise e

                try:
                    logger.debug(f"Parsing event from message {message_key}...")
                    event = parse_fire_event(event_dict)
                except Exception as e:
                    messages_with_errors += 1
                    logger.error(f"Failed to create FireEvent from dict for message {message_key}: {e}")
                    logger.error(f"Failed {message_key} body: {event_dict}")

                    if ON_FAILURE == "continue":
                        continue
                    elif ON_FAILURE == "raise":
                        raise e
                    break
                try:
                    issues = data_quality_analysis(event)
                    if not issues.keys():
                        sucessful_messages += 1
                        logger.debug(f"Event {message_key} passed data quality checks.")
                        producer.produce(
                            VALIDATED_EVENTS_TOPIC,
                            key=message_key,
                            value=json.dumps(event_dict),
                        )

                        latest_successful_event = event

                    else:
                        messages_with_errors += 1
                        logger.warning(f"Event {message_key} failed data quality checks: {issues}")
                        event_dict["data_quality_issues"] = issues
                        producer.produce(
                            UNVALIDATED_EVENTS_TOPIC,
                            key=message_key,
                            value=json.dumps(event_dict),
                        )
                except Exception as e:
                    logger.error(f"Data quality analysis failed for event {message_key}: {e}")
                    if ON_FAILURE == "continue":
                        producer.produce(
                            UNVALIDATED_EVENTS_TOPIC,
                            key=message_key,
                            value=json.dumps(event_dict),
                        )
                        continue
                    elif ON_FAILURE == "raise":
                        raise e
                    producer.flush(FLUSH_TIMEOUT)
                    break
                processed_messages += 1
                hline(header=str(message_key), char="=", as_debug=True)

            hline()
            logger.info(f"Processed {processed_messages} messages in this batch.")
            logger.info(f"Successfully validated {sucessful_messages} messages.")
            logger.info(f"Messages with errors: {messages_with_errors}.")
            logger.info(
                f"Latest successful event ID: {str(latest_successful_event.ID if latest_successful_event else "N/A")}"
            )
            logger.info(
                f"Latest successful event Date: {try_strftime(latest_successful_event.Incident_Date, EFFECTIVE_DATETIME_FORMATS ) if latest_successful_event else "N/A"}"
            )
            logger.info(
                f"Latest successful event Number: {latest_successful_event.Incident_Number if latest_successful_event else ""}"
            )
            hline()
            hline(as_debug=True)
            logger.debug(
                f"{effective_consumer_group} lag for {EVENTS_SOURCE_TOPIC}: {lag}"
            )
            hline(as_debug=True)

            producer.flush(
                FLUSH_TIMEOUT
            )  # Flush the producer to ensure messages are sent

        except Exception as e:
            logger.error(f"Error in Fire Event Data Quality Service: {e}")
            break

        if not MAIN_LOOP:
            break

        time.sleep(MAIN_LOOP_INTERVAL)


if __name__ == "__main__":
    main()
