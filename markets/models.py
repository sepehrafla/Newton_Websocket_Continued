from django.db import models
import redis
from django.conf import settings
import time
import logging
import json

logger = logging.getLogger(__name__)

class PriceHistory:
    def __init__(self):
        logger.info("Initializing Redis connection")
        try:
            self.redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()  # Test connection
            logger.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
            
        self.price_key_format = "price_history:{symbol}"
        self.week_seconds = 7 * 24 * 60 * 60

    def store_price(self, symbol: str, price_data: dict):
        """Store price data with timestamp"""
        key = self.price_key_format.format(symbol=symbol)
        try:
            # Log the data being stored
            logger.debug(f"Storing price for {symbol}: {json.dumps(price_data)}")
            
            # Store the data
            result = self.redis_client.zadd(key, {str(price_data): price_data['timestamp']})
            
            # Cleanup old data
            month_ago = int(time.time()) - (30 * 24 * 60 * 60)
            removed = self.redis_client.zremrangebyscore(key, '-inf', month_ago)
            
            logger.info(f"Stored price for {symbol}: added={result}, removed={removed} old entries")
            
            # Log current data count
            count = self.redis_client.zcard(key)
            logger.debug(f"Current price history count for {symbol}: {count}")
            
        except redis.RedisError as e:
            logger.error(f"Redis error storing price for {symbol}: {str(e)}")
            raise

    def get_previous_price(self, symbol: str, window: int = None) -> dict:
        """Get previous price data for change calculation"""
        key = self.price_key_format.format(symbol=symbol)
        window = window or self.week_seconds
        current_time = int(time.time())
        previous_time = current_time - window
        
        try:
            logger.debug(f"Fetching previous price for {symbol} from {previous_time} to {current_time}")
            
            prices = self.redis_client.zrangebyscore(
                key, 
                previous_time, 
                current_time, 
                start=0, 
                num=1
            )
            
            if prices:
                price_data = eval(prices[0])
                logger.info(f"Found previous price for {symbol}: {json.dumps(price_data)}")
                return price_data
            else:
                logger.warning(f"No previous price found for {symbol} in the last {window} seconds")
                return None
                
        except redis.RedisError as e:
            logger.error(f"Redis error fetching previous price for {symbol}: {str(e)}")
            raise
        except (ValueError, SyntaxError) as e:
            logger.error(f"Error parsing price data for {symbol}: {str(e)}")
            return None
