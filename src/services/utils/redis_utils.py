import os
import redis
from datetime import datetime
from src.services.utils.logger_utils import getLogger
from redis.commands.search.field import TagField, NumericField, TextField
from redis.commands.search.index_definition import IndexDefinition
from functools import lru_cache

logger = getLogger(__file__)

HOST = os.getenv("REDIS_HOST", "redis")
PORT = int(os.getenv("REDIS_PORT", 6379))
DB = int(os.getenv("REDIS_DB", 0))
PASSWORD = os.getenv("REDIS_PASSWORD", None)


@lru_cache(maxsize=1)
def get_redis_client(host=HOST, port=PORT, db=DB, password=PASSWORD) -> redis.Redis:
    """
    Returns a singleton Redis client configured with the provided parameters.

    :param host: Redis server host
    :param port: Redis server port
    :param db: Redis database number
    :param password: Redis server password (if any)
    :return: Redis client instance
    """
    logger.debug(f"Connecting to Redis at {host}:{port}, DB: {db}")
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=True,  # ensures responses are strings
    )


def delete_keys(query: str) -> None:
    """
    Deletes keys from Redis that match the given query pattern.
    :param query: Redis key pattern to match (e.g., "user:*")
    """
    global logger
    r = get_redis_client()
    logger.debug(f"Deleting keys matching pattern: {query}")
    deleted_count = 0
    items_iterator = r.scan_iter(query)
    if not items_iterator:
        logger.debug(f"No keys found matching pattern '{query}'.")
        return
    for item in items_iterator:    
        r.delete(item)
        logger.debug(f"Deleted key: {item}")
        deleted_count += 1

    if deleted_count == 0:
        logger.debug(f"No keys found matching pattern '{query}'.")


def index_exists(id):
    r = get_redis_client()

    existing_indexes = r.execute_command("FT._LIST")
    return id in existing_indexes


def delete_index(index_id):
    try:
        r = get_redis_client()

        # Check if the index exists in the list of indices
        if index_exists(index_id):
            r.ft(index_id).dropindex(delete_documents=False)
            logger.debug(f"Index '{index_id}' dropped successfully.")
        else:
            logger.debug(f"Index '{index_id}' does not exist. No action taken.")

    except Exception as e:
        logger.error("Error while deleting index:", e)


def create_index(id, schema, prefixes: list[str]):

    if not index_exists(id):
        r = get_redis_client()

        r.ft(id).create_index(
            fields=schema, definition=IndexDefinition(prefix=prefixes)
        )

        logger.debug(f"Index {id} created successfully.")


def store_as_hash(key: str, data: dict):
    rcli = get_redis_client()
    # Serialize datetime fields to ISO format
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.timestamp()
        elif v is None:
            data[k] = ""  # Store empty string for None values
        else:
            data[k] = str(v)  # Convert to string for Redis

    # Store in Redis as a hash
    rcli.hset(key, mapping=data)


def get_latest_revision(key_prefix):
    """
    Retrieve the latest revision of the key pattern f"{r_event_key_prefix}:{revision}".
    """
    # Use the SCAN command to safely iterate over keys (instead of KEYS which blocks Redis)
    rclient = get_redis_client()
    latest_revision = None
    highest_revision = -1

    items_iterator = rclient.scan_iter(f"{key_prefix}:*")
    for key in items_iterator:
        key_str = key
        try:
            # Extract the revision number from the key
            revision_str = key_str.split(":")[-1]
            revision = int(revision_str)
            if revision > highest_revision:
                highest_revision = revision
                latest_revision = key_str
        except (ValueError, IndexError) as err:
            # Skip keys that don't follow the expected pattern
            logger.debug(f"skipping: {key_str}")
            continue

    return highest_revision
