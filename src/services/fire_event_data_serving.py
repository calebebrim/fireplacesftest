import os
import json
import time
import datetime

from typing import Optional
from datetime import datetime
from dataclasses import asdict

from src.services.utils.logger_utils import getLogger, hline
from src.services.models.fire_event import FireEvent, parse_fire_event, fire_event_to_key
from src.services.utils.kafka_utils import create_kafka_consumer, create_consumer_config, kafka_consumer_generator
from src.services.utils.redis_utils import (
    get_redis_client,
    TagField,
    NumericField,
    TextField,
    create_index,
    delete_index,
    store_as_hash,
    get_latest_revision,
    delete_keys,
)

logger = getLogger(__file__)

SERVICE_NAME = os.environ.get("SERVICE_NAME", "fire_event_data_serving")

REDIS_EVENT_KEY_PREFIX = f"fireevent"
REDIS_EVENT_INDEX_ID = f"{os.environ.get("REDIS_EVENT_INDEX_ID","fireevent")}_idx"

ON_FAILURE = os.environ.get("ON_FAILURE", "continue")
ON_DUPLICATE = os.environ.get("ON_DUPLICATE", "continue").lower()
if ON_DUPLICATE not in "version,replace,continue,fail": raise ValueError(f"Unknown ON_DUPLICATE option: {ON_DUPLICATE}")


DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y/%m/%d")
DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT", "%Y/%m/%d %H:%M:%S")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
MAIN_LOOP = os.environ.get("MAIN_LOOP", "True").lower() == "true"
MAIN_LOOP_INTERVAL = int(os.environ.get("MAIN_LOOP_INTERVAL", 30))
MAIN_LOOP_TIMEOUT = int(os.environ.get("MAIN_LOOP_TIMEOUT", 60))

RESTART = os.environ.get("RESTART", "False").lower() == "true"

VALIDATED_EVENTS_TOPIC = os.getenv("VALIDATED_EVENTS_TOPIC", "validated-fire-events")
VALIDATED_EVENTS_TOPIC_CG = os.getenv("VALIDATED_EVENTS_TOPIC_CG", SERVICE_NAME)
rcli = get_redis_client()

def create_indexes():
    """
        create all required indexes for the serving layer
    """
    schema = [
        TagField("Incident_Number"),  # Grouping/filtering by incident number
        TagField("neighborhood_district"),  # District as tag field
        TagField("Battalion"),  # Battalion as tag field
        NumericField("ID", sortable=True),  # Row ID as numeric, sortable
        NumericField("Alarm_DtTm", sortable=True),  # ISO date-time string, sortable
        NumericField("Incident_Date", sortable=True),  # ISO date-time string, sortable
    ]
    create_index(REDIS_EVENT_INDEX_ID, schema, prefixes=[f"{REDIS_EVENT_KEY_PREFIX}"])


def recreate_indexes():
    delete_index(REDIS_EVENT_INDEX_ID)
    create_indexes()

def store_fire_event(event: FireEvent) -> None:
    """
    Stores a FireEvent dataclass instance in Redis as a hash.
    Uses the key pattern: fireevent:{ID}
    Serializes datetime fields as ISO strings.
    """
    r_event_key: str = fire_event_to_key(event, REDIS_EVENT_KEY_PREFIX)

    if bool(rcli.exists(f"{r_event_key}:0")):
        # version,replace,continue,fail
        if ON_DUPLICATE == "fail":
            raise ValueError(f"Duplicated event detected {r_event_key}:0")
        elif ON_DUPLICATE == "continue":
            logger.debug(f"skipping {r_event_key}:0")
            return None
        elif ON_DUPLICATE in ["replace", "version"]:
            if ON_DUPLICATE == "replace":
                _k = f"{r_event_key}:0"
                logger.debug(f"Overriding event {_k}")
            elif ON_DUPLICATE == "version":
                latest_revision = get_latest_revision(r_event_key)
                _k = f"{r_event_key}:{latest_revision+1}"
                logger.debug(f"Creating a new version {_k}")

            store_as_hash(_k, asdict(event))
        else:
            return logger.warning(f"Impossible to parse {ON_DUPLICATE} to ON_DUPLICATE.")
    else:
        _k = f"{r_event_key}:0"
        logger.debug(f"persisting: {_k}")
        return store_as_hash(_k, asdict(event))


def main():
    if RESTART:
        recreate_indexes()
        delete_keys(REDIS_EVENT_KEY_PREFIX)
        hline()
        logger.info("RESTARTED")
        hline()
        return 
    create_indexes()
    kc = create_kafka_consumer(create_consumer_config(consumer_group=VALIDATED_EVENTS_TOPIC_CG), [VALIDATED_EVENTS_TOPIC])
    logger.info(f"{SERVICE_NAME} is started.")
    while True:
        processed_messages = 0  
        messages_with_errors = 0
        sucessful_messages = 0
        latest_successful_event = None
        latest_incident_time = None
        latest_sucessful_incident_time = None
        start_time = time.time()

        def stop():
            end_time = time.time()
            elapsed_time = end_time - start_time
            timeout = elapsed_time > MAIN_LOOP_TIMEOUT
            if timeout:
                logger.info(f"Main loop timedout after {elapsed_time}s")
            return processed_messages >= BATCH_SIZE or timeout

        try:
            for msg in kafka_consumer_generator(kc):
                if stop():
                    break
                key_str = msg.key().decode("utf-8")
                hline(header=key_str, as_debug=True)
                processed_messages+=1
                data_str = msg.value().decode("utf-8")
                data = json.loads(data_str)
                event: FireEvent = parse_fire_event(data)
                latest_incident_time = event.Incident_Date

                store_fire_event(event)

                latest_successful_event = key_str
                sucessful_messages +=1
                latest_sucessful_incident_time = event.Incident_Date
                hline(header=key_str, as_debug=True)
        except Exception as err: 
            messages_with_errors+=1
            if ON_FAILURE.lower() == "raise":
                raise err
        hline(char="*", header="Process Report")
        logger.info(f"Processed messages: {processed_messages}")
        logger.info(f"Sucessfull messages: {sucessful_messages}")
        logger.info(f"Messages with errors: {messages_with_errors}")
        logger.info(f"Latest sucessfull event: {latest_successful_event}")
        logger.info(f"Latest incident time: {latest_incident_time}")
        logger.info(f"Latest sucessfull incident time: {latest_sucessful_incident_time}")
        hline(char="*")

        if not MAIN_LOOP: 
            break


if __name__ == "__main__":
    main()
