import os
import redis

from src.services.utils.logger_utils import getLogger

logger = getLogger(__file__)

HOST = os.getenv("REDIS_HOST", "redis")
PORT = int(os.getenv("REDIS_PORT", 6379))
DB = int(os.getenv("REDIS_DB", 0))
PASSWORD = os.getenv("REDIS_PASSWORD", None)

def get_redis_client(host=HOST, port=PORT, db=DB, password=PASSWORD) -> redis.Redis:
    """
    Returns a Redis client configured with the provided parameters.
    
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
