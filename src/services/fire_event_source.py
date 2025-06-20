import os
import time
import json
import logging
import asyncio

from datetime import datetime
from src.services.utils.logger_utils import getLogger, hline
from src.services.utils.csv_utils import from_csv_generator
from src.services.utils.redis_utils import get_redis_client, redis, delete_keys
from src.services.utils.kafka_utils import create_kafka_producer, create_producer_config, create_kafka_topic_if_not_exists, delete_kafka_topic
from src.services.utils.dateutils import try_strptime
from confluent_kafka import Producer

from typing import Optional

ON_FAILURE = os.environ.get("ON_FAILURE", "continue")
DATE_FORMAT = os.environ.get("DATE_FORMAT", "%Y/%m/%d")
DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT", "%Y/%m/%d %H:%M:%S")
START_DATE = datetime.strptime(os.environ.get("START_DATE", "2021/01/01"), DATE_FORMAT)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
MAIN_LOOP = os.environ.get("MAIN_LOOP", "True").lower() == "true"
MAIN_LOOP_INTERVAL = int(os.environ.get("MAIN_LOOP_INTERVAL", 30))
CSV_FOLDER_PATH = os.environ.get("CSV_FOLDER_PATH", "/data/fire_events")
FIRE_EVENT_SOURCE_TOPIC = os.environ.get("FIRE_EVENT_SOURCE_TOPIC", "fire_event_source")
SERVICE_NAME = os.environ.get("SERVICE_NAME", "fire_event_source")
RESTART = os.environ.get("RESTART", "False").lower() == "true"
REDIS_LAST_EVENT_TIMESTAMP_KEY = os.environ.get("REDIS_LATEST_EVENT_TIMESTAMP", f"{SERVICE_NAME}:latest_event_timestamp")

logger = getLogger(__file__)


#


def redis_row_key(row: dict):
    return f"{SERVICE_NAME}:message:{row['ID']}"


def redis_file_key(file: str):
    return f"{SERVICE_NAME}:file:{file}"


def main():
    global START_DATE
    logger.info("Starting fire event source...")

    logger.info(
        f"Starting fire event source with environment: "
        f"\nSERVICE_NAME={SERVICE_NAME}, "
        f"\nFIRE_EVENT_SOURCE_TOPIC={FIRE_EVENT_SOURCE_TOPIC}, "
        f"\nON_FAILURE={ON_FAILURE}, "
        f"\nDATE_FORMAT={DATE_FORMAT}, "
        f"\nDATETIME_FORMAT={DATETIME_FORMAT}, "
        f"\nBATCH_SIZE={BATCH_SIZE}, "
        f"\nMAIN_LOOP={MAIN_LOOP}, "
        f"\nMAIN_LOOP_INTERVAL={MAIN_LOOP_INTERVAL}, "
        f"\nCSV_FOLDER_PATH={CSV_FOLDER_PATH}, "
        f"\nSTART_DATE={START_DATE}, "
        f"\nRESTART={RESTART}, "
        # f"\nlatest_event_timestamp={latest_event_timestamp}"
    )
    time.sleep(10)

    rcli: redis.Redis = get_redis_client()  # Initialize Redis client
    def control_delivery_report(err, msg):
        """
        Callback function to report the delivery status of messages.
        :param err: Error if any, None if successful.
        :param msg: The message that was sent.
        """
        try: 
            if err is not None:
                logger.error(f"Message delivery failed: {err}")
            else:
                logger.debug(f"Message {msg.key().decode('utf-8')} delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                row = json.loads(msg.value())
                rkey = redis_row_key(row)  # Use the key from the message

                logger.debug(f"Setting control key in Redis: {rkey}")
                rcli.set(rkey, json.dumps({"processed": True}))  # Store the message in Redis

                # retrieve the latest event timestamp from Redis
                latest_event_timestamp: Optional[datetime] = None
                latest_event_timestamp_str = None
                if rcli.exists(REDIS_LAST_EVENT_TIMESTAMP_KEY):
                    latest_event_timestamp_str = str(rcli.get(REDIS_LAST_EVENT_TIMESTAMP_KEY))
                    logger.debug(f"Latest event timestamp from Redis: {latest_event_timestamp_str}")
                    latest_event_timestamp = try_strptime(
                        latest_event_timestamp_str, [DATETIME_FORMAT, DATE_FORMAT]
                    )

                incident_date_str = row.get("Incident Date", "")
                incident_date = try_strptime(
                    incident_date_str, [DATETIME_FORMAT, DATE_FORMAT]
                )
                if incident_date is None:
                    raise ValueError(
                        f"Invalid incident date format for incident {row.get('Incident Number', 'N/A')}: {incident_date_str}"
                    )                                                                
                # datetime.strptime(
                #     incident_date_str, DATETIME_FORMAT
                # )
                if latest_event_timestamp is None:
                    logger.debug(
                        f"Setting latest event timestamp to {incident_date_str} for incident {row.get('Incident Number', 'N/A')}"
                    )
                    rcli.set(REDIS_LAST_EVENT_TIMESTAMP_KEY, incident_date_str)
                elif incident_date > latest_event_timestamp:
                    logger.debug(
                        f"Updating latest event timestamp from {latest_event_timestamp_str} to {incident_date_str} for incident {row.get('Incident Number', 'N/A')}"
                    )
                    rcli.set(
                        REDIS_LAST_EVENT_TIMESTAMP_KEY,
                        incident_date_str
                    )
        except Exception as e:
            logger.error(f"Error in control delivery report: {e}")
            raise e

    producer_config = create_producer_config()
    kprod: Producer = create_kafka_producer(producer_config)

    if RESTART:
        logger.info("Restarting fire event source...")
        logger.info("Deleting latest event timestamp key from Redis.")
        rcli.delete(REDIS_LAST_EVENT_TIMESTAMP_KEY)
        # Query: find all keys matching a pattern (e.g., "user:*")
        all_messages_keys = f"{SERVICE_NAME}:message:*"
        all_files_keys = f"{SERVICE_NAME}:file:*"
        # Delete all found keys
        logger.info(f"Deleting all keys matching pattern: {all_messages_keys}")
        delete_keys(all_messages_keys)
        logger.info(f"Deleting all keys matching pattern: {all_files_keys}")
        delete_keys(all_files_keys)

        delete_kafka_topic(producer_config, FIRE_EVENT_SOURCE_TOPIC)
        hline()
        logger.warning("<RESTARTED>")
        hline()
        return
    create_kafka_topic_if_not_exists(
        config=producer_config, 
        topic_name=FIRE_EVENT_SOURCE_TOPIC,
        num_partitions=30,
        replication_factor=1
    )

    while True:
        # Example usage

        files = os.listdir(CSV_FOLDER_PATH)  # Ensure the file exists

        if not files:
            raise FileNotFoundError(f"No files found in the directory: {CSV_FOLDER_PATH}")

        logger.info(f"Files found in {CSV_FOLDER_PATH}: {files}")

        rows = []
        latest_event_timestamp: Optional[datetime] = None
        if rcli.exists(REDIS_LAST_EVENT_TIMESTAMP_KEY):
            logger.info(
                f"Retrieving latest event timestamp from Redis: {rcli.get(REDIS_LAST_EVENT_TIMESTAMP_KEY)}"
            )
            latest_event_timestamp = try_strptime(
                str(rcli.get(REDIS_LAST_EVENT_TIMESTAMP_KEY)),
                [DATETIME_FORMAT, DATE_FORMAT],
            )

        # Ensure START_DATE is not earlier than the latest event timestamp
        if latest_event_timestamp:
            START_DATE = max(START_DATE, latest_event_timestamp) 

        batch = BATCH_SIZE
        processed_rows = 0
        read_rows = 0
        rkey = None
        latest_key_produced = None
        for file in files:
            logger.info(f"Processing file: {file}")
            csv_file_path = os.path.join(CSV_FOLDER_PATH, file)  # Get the first file in the directory

            # ================= checking file redis key
            rfilek = redis_file_key(csv_file_path)
            if not rcli.exists(rfilek):
                rcli.set(rfilek, json.dumps({"latest_row": 0, "completed": False}))
            file_status: dict = json.loads(str(rcli.get(rfilek)))
            if file_status.get("completed", False):
                logger.debug(f"file completed: {csv_file_path}: {file_status} ")
                continue
            # ================= checking file redis key
            for row in from_csv_generator(csv_file_path):
                if int(file_status.get("latest_row", 0)) > int(row.get("ID", 0)):
                    logger.debug(
                        f"skipping already processed row id: {row.get("ID", 0)}"
                    )
                    continue
                if "_end_" in row.keys():
                    # all rows readed, marking it as completed.
                    _s = json.loads(str(rcli.get(rfilek)))
                    _s["completed"] = True
                    rcli.set(rfilek, json.dumps(_s))
                    break
                try:
                    incident_date_str = str(row["Incident Date"])
                    if incident_date_str:
                        incident_date = datetime.strptime(
                            incident_date_str, DATE_FORMAT
                        )
                    else:
                        # incident need to have a Date
                        continue
                    if (read_rows % 100000) == 0:
                        hline()
                        logger.info(f"Read {read_rows} rows so far.")
                        logger.info(f"Current row ID: {row.get('ID', 'N/A')}")
                        logger.info(f"Latest event timestamp: {rcli.get(REDIS_LAST_EVENT_TIMESTAMP_KEY) if rcli.exists(REDIS_LAST_EVENT_TIMESTAMP_KEY) else 'N/A'}")
                        logger.info(f"Current row incident date: {incident_date}")
                        logger.info(f"filtering rows with incident date >= {START_DATE}: {incident_date >= START_DATE}")
                        hline()

                    if incident_date >= START_DATE:

                        rkey = redis_row_key(
                            row
                        )  # to make sure all rows are processed.
                        key = str(
                            row.get("Incident Number")
                        )  # ensure all Incident Number are producer in the same topic partition ensuring sequenciality.
                        value = None
                        try:
                            value = json.dumps(row)
                        except TypeError as err:
                            logger.error(f"Error serializing row {key}: {str(err)}")
                            if ON_FAILURE == "continue":
                                continue
                            elif ON_FAILURE == "raise":
                                raise err
                        logger.debug(f"Checking Redis for control key: {rkey}")
                        if rcli.exists(rkey):
                            control_data = str(
                                rcli.get(rkey)
                            )
                            control = json.loads(control_data)
                            if control and control.get("processed"):
                                logger.debug(f"Skipping already processed row {key}.")
                                continue
                        logger.debug(
                            f"Producing row {key} with date {incident_date} to topic {FIRE_EVENT_SOURCE_TOPIC}."
                        )
                        kprod.produce(
                            FIRE_EVENT_SOURCE_TOPIC,
                            key=key,  #
                            value=value,
                            callback=control_delivery_report,
                        )
                        # will only set the latest_key_produced if reach this point.
                        latest_key_produced = key

                        if processed_rows >= batch:
                            logger.info(f"Flushing producer after processing {processed_rows} rows...")

                            kprod.flush(1)
                            logger.info(f"Flushing completed.")

                            break
                        processed_rows += 1

                    read_rows += 1
                except ValueError as err:
                    logger.error(f"Invalid incident {key} error: {str(err)}.")
                    logger.debug(row)
                    if ON_FAILURE == "continue":
                        continue
                    elif ON_FAILURE == "raise":
                        raise err
                    kprod.flush(1)

            hline()
            file_status["latest_row"] = row.get("ID")
            logger.info(f"Read {read_rows} rows from {len(files)} files.")
            logger.info(f"Total rows processed: {processed_rows}")
            logger.info(
                f"Latest event timestamp: {rcli.get(REDIS_LAST_EVENT_TIMESTAMP_KEY) if rcli.exists(REDIS_LAST_EVENT_TIMESTAMP_KEY) else 'N/A'}"
            )
            logger.info(f"Latest redis key set: {rkey if rkey else 'N/A'}")
            logger.info(
                f"Latest key produced: {latest_key_produced if latest_key_produced else 'N/A'}"
            )

            hline()
            logger.info(
                f"Waiting for {MAIN_LOOP_INTERVAL} seconds before the next iteration."
            )

        if not MAIN_LOOP:
            break
        time.sleep(MAIN_LOOP_INTERVAL)


if __name__ == "__main__":
    main()
