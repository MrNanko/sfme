#!/usr/bin/env python3
# _*_ coding:utf-8 _*_

# @Time       : 2025/6/11 15:10
# @Author     : @MrNanko
# @File       : redis
# @Software   : PyCharm
# @Description: Redis client and operations management

import redis
import json
import pickle
import logging
from typing import Any, Optional, Dict, List

logger = logging.getLogger(__name__)

# Global Redis instances
redis_manager = None
redis_ops = None

def init_redis_client(host='localhost', port=6379, db=0, password=None,
                     decode_responses=True, socket_timeout=5, retry_on_timeout=True):
    """
    Initialize Redis client and set global instances
    
    Args:
        host: Redis server host
        port: Redis server port
        db: Redis database number
        password: Redis password
        decode_responses: Whether to decode responses
        socket_timeout: Socket timeout
        retry_on_timeout: Whether to retry on timeout
    """
    global redis_manager, redis_ops
    try:
        # Create Redis manager instance
        redis_manager = RedisManager(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
            socket_timeout=socket_timeout,
            retry_on_timeout=retry_on_timeout
        )
        
        # Create Redis operations instance
        redis_ops = RedisOperations(redis_manager)
        
        # Test connection
        if redis_manager.ping():
            logger.info("Redis connection initialized successfully")
            return redis_ops
        else:
            raise ConnectionError("Failed to connect to Redis")
            
    except Exception as e:
        logger.error(f"Failed to initialize Redis: {e}")
        raise

class RedisManager:
    """Redis Connection Manager"""

    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 decode_responses=True, socket_timeout=5, retry_on_timeout=True):
        """
        Initialize Redis connection

        Args:
            host: Redis server address
            port: Redis port
            db: Database number
            password: Password
            decode_responses: Whether to automatically decode responses
            socket_timeout: Connection timeout
            retry_on_timeout: Whether to retry on timeout
        """
        self.connection_params = {
            'host': host,
            'port': port,
            'db': db,
            'password': password,
            'decode_responses': decode_responses,
            'socket_timeout': socket_timeout,
            'retry_on_timeout': retry_on_timeout
        }

        self.redis_client = None
        self._connect()

    def _connect(self):
        """Establish Redis connection"""
        try:
            self.redis_client = redis.Redis(**self.connection_params)
            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection successful")
        except redis.ConnectionError as e:
            logger.error(f"Redis connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Redis initialization failed: {e}")
            raise

    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False

    def close(self):
        """Close Redis connection"""
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")


# =================== Basic Redis Operations ===================

class RedisOperations:
    """Redis Basic Operations Wrapper"""

    def __init__(self, redis_manager: RedisManager):
        self.redis = redis_manager.redis_client

    # ================ String Operations ================

    def set_string(self, key: str, value: str, ex: int = None) -> bool:
        """
        Set string value

        Args:
            key: Key name
            value: Value
            ex: Expiration time (seconds)
        """
        try:
            return self.redis.set(key, value, ex=ex)
        except Exception as e:
            logger.error(f"Failed to set string {key}: {e}")
            return False

    def get_string(self, key: str) -> Optional[str]:
        """Get string value"""
        try:
            return self.redis.get(key)
        except Exception as e:
            logger.error(f"Failed to get string {key}: {e}")
            return None

    def delete_key(self, key: str) -> bool:
        """Delete key"""
        try:
            return bool(self.redis.delete(key))
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return bool(self.redis.exists(key))
        except Exception as e:
            logger.error(f"Failed to check key existence {key}: {e}")
            return False

    def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration time"""
        try:
            return bool(self.redis.expire(key, seconds))
        except Exception as e:
            logger.error(f"Failed to set expiration time {key}: {e}")
            return False

    def ttl(self, key: str) -> int:
        """Get remaining time to live for key"""
        try:
            return self.redis.ttl(key)
        except Exception as e:
            logger.error(f"Failed to get TTL {key}: {e}")
            return -1

    # ================ JSON Operations ================

    def set_json(self, key: str, value: Any, ex: int = None) -> bool:
        """Set JSON value"""
        try:
            json_str = json.dumps(value, ensure_ascii=False)
            return self.redis.set(key, json_str, ex=ex)
        except Exception as e:
            logger.error(f"Failed to set JSON {key}: {e}")
            return False

    def get_json(self, key: str) -> Optional[Any]:
        """Get JSON value"""
        try:
            json_str = self.redis.get(key)
            if json_str:
                return json.loads(json_str)
            return None
        except Exception as e:
            logger.error(f"Failed to get JSON {key}: {e}")
            return None

    # ================ Object Serialization Operations ================

    def set_object(self, key: str, obj: Any, ex: int = None) -> bool:
        """Set Python object (using pickle)"""
        try:
            pickled_obj = pickle.dumps(obj)
            return self.redis.set(key, pickled_obj, ex=ex)
        except Exception as e:
            logger.error(f"Failed to set object {key}: {e}")
            return False

    def get_object(self, key: str) -> Optional[Any]:
        """Get Python object"""
        try:
            pickled_obj = self.redis.get(key)
            if pickled_obj:
                return pickle.loads(pickled_obj)
            return None
        except Exception as e:
            logger.error(f"Failed to get object {key}: {e}")
            return None

    # ================ Hash Operations ================

    def hset(self, name: str, key: str, value: str) -> bool:
        """Set hash field"""
        try:
            return bool(self.redis.hset(name, key, value))
        except Exception as e:
            logger.error(f"Failed to set hash {name}.{key}: {e}")
            return False

    def hget(self, name: str, key: str) -> Optional[str]:
        """Get hash field"""
        try:
            return self.redis.hget(name, key)
        except Exception as e:
            logger.error(f"Failed to get hash {name}.{key}: {e}")
            return None

    def hgetall(self, name: str) -> Dict:
        """Get all hash fields"""
        try:
            return self.redis.hgetall(name)
        except Exception as e:
            logger.error(f"Failed to get all hash fields {name}: {e}")
            return {}

    def hmset(self, name: str, mapping: Dict) -> bool:
        """Batch set hash fields"""
        try:
            return bool(self.redis.hmset(name, mapping))
        except Exception as e:
            logger.error(f"Failed to batch set hash fields {name}: {e}")
            return False

    # ================ List Operations ================

    def lpush(self, name: str, *values) -> int:
        """Push elements to the left of the list"""
        try:
            return self.redis.lpush(name, *values)
        except Exception as e:
            logger.error(f"Left push failed {name}: {e}")
            return 0

    def rpush(self, name: str, *values) -> int:
        """Push elements to the right of the list"""
        try:
            return self.redis.rpush(name, *values)
        except Exception as e:
            logger.error(f"Right push failed {name}: {e}")
            return 0

    def lpop(self, name: str) -> Optional[str]:
        """Pop elements from the left of the list"""
        try:
            return self.redis.lpop(name)
        except Exception as e:
            logger.error(f"Left pop failed {name}: {e}")
            return None

    def rpop(self, name: str) -> Optional[str]:
        """Pop elements from the right of the list"""
        try:
            return self.redis.rpop(name)
        except Exception as e:
            logger.error(f"Right pop failed {name}: {e}")
            return None

    def llen(self, name: str) -> int:
        """Get the length of the list"""
        try:
            return self.redis.llen(name)
        except Exception as e:
            logger.error(f"Failed to get list length {name}: {e}")
            return 0

    def lrange(self, name: str, start: int, end: int) -> List:
        """Get range of elements from the list"""
        try:
            return self.redis.lrange(name, start, end)
        except Exception as e:
            logger.error(f"Failed to get list range {name}: {e}")
            return []

    # ================ Set Operations ================

    def sadd(self, name: str, *values) -> int:
        """Add elements to a set"""
        try:
            return self.redis.sadd(name, *values)
        except Exception as e:
            logger.error(f"Failed to add elements to set {name}: {e}")
            return 0

    def srem(self, name: str, *values) -> int:
        """Remove elements from a set"""
        try:
            return self.redis.srem(name, *values)
        except Exception as e:
            logger.error(f"Failed to remove elements from set {name}: {e}")
            return 0

    def smembers(self, name: str) -> set:
        """Get all elements in a set"""
        try:
            return self.redis.smembers(name)
        except Exception as e:
            logger.error(f"Failed to get set elements {name}: {e}")
            return set()

    def sismember(self, name: str, value: str) -> bool:
        """Check if element exists in a set"""
        try:
            return bool(self.redis.sismember(name, value))
        except Exception as e:
            logger.error(f"Failed to check set membership {name}: {e}")
            return False

    # ================ Sorted Set Operations ================

    def zadd(self, name: str, mapping: Dict[str, float]) -> int:
        """Add elements to a sorted set"""
        try:
            return self.redis.zadd(name, mapping)
        except Exception as e:
            logger.error(f"Failed to add elements to sorted set {name}: {e}")
            return 0

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> List:
        """Get range of elements from a sorted set"""
        try:
            return self.redis.zrange(name, start, end, withscores=withscores)
        except Exception as e:
            logger.error(f"Failed to get range from sorted set {name}: {e}")
            return []

    def zrem(self, name: str, *values) -> int:
        """Remove elements from a sorted set"""
        try:
            return self.redis.zrem(name, *values)
        except Exception as e:
            logger.error(f"Failed to remove elements from sorted set {name}: {e}")
            return 0
