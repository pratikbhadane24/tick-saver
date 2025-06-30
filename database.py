import logging
import os

import redis
from dotenv import load_dotenv

load_dotenv()

redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', '6379')
redis_password = os.environ.get('REDIS_PASSWORD', None)
redis_db_no = os.environ.get('REDIS_DB_NO', 0)
candle_redis_uri = os.environ.get('CANDLE_REDIS_URL')

try:
    REDIS_DB_CLIENT = redis.Redis(host=redis_host, port=int(redis_port), password=redis_password,
                                  max_connections=1000, db=redis_db_no)
    if not REDIS_DB_CLIENT.ping():
        raise ConnectionError("Failed to connect to Redis DB")
    logging.info("Connected to Redis DB successfully")
except Exception as e:
    logging.error(f"Error connecting to Redis DB: {e}")


try:
    REDIS_DATA_STORE = redis.from_url(candle_redis_uri)
    if not REDIS_DATA_STORE.ping():
        raise ConnectionError("Failed to connect to Redis data store")
    logging.info("Connected to Redis data store successfully")
except Exception as e:
    logging.error(f"Error connecting to Redis DB: {e}")