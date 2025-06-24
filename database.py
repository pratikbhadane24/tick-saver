import logging
import os

import redis
from dotenv import load_dotenv

load_dotenv()

redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', '6379')
redis_password = os.environ.get('REDIS_PASSWORD', None)
redis_db_no = os.environ.get('REDIS_DB_NO', 0)

try:
    REDIS_DB_CLIENT = redis.Redis(host=redis_host, port=int(redis_port), password=redis_password,
                                  max_connections=1000, db=redis_db_no)
except Exception as e:
    logging.error(f"Error connecting to Redis DB: {e}")
