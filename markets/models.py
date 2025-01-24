from redis.asyncio import Redis
from django.conf import settings
import time
import logging
import json
import asyncio

logger = logging.getLogger(__name__)

class PriceHistory:
    def __init__(self):
        logger.info("Initializing Redis connection")
        try:
            self.redis_client = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.pubsub = self.redis_client.pubsub()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise
            
        self.price_key_format = "price_history:{symbol}"
        self.price_channel = "price_updates"  # Channel for price updates
        self.week_seconds = 7 * 24 * 60 * 60

    async def store_price(self, symbol: str, price_data: dict):
        """Store price data with timestamp and publish update"""
        key = self.price_key_format.format(symbol=symbol)
        try:
            logger.debug(f"Storing price for {symbol}: {json.dumps(price_data)}")
            
            # Store the data in sorted set
            result = await self.redis_client.zadd(key, {str(price_data): price_data['timestamp']})
            
            # Publish the update
            message = {
                "event": "price_update",
                "symbol": symbol,
                "data": price_data
            }
            await self.redis_client.publish(self.price_channel, json.dumps(message))
            logger.debug(f"Published price update for {symbol}")
            
            # Cleanup old data
            month_ago = int(time.time()) - (30 * 24 * 60 * 60)
            removed = await self.redis_client.zremrangebyscore(key, '-inf', month_ago)
            
            logger.info(f"Stored price for {symbol}: added={result}, removed={removed} old entries")
            
            count = await self.redis_client.zcard(key)
            logger.debug(f"Current price history count for {symbol}: {count}")
            
        except Exception as e:
            logger.error(f"Redis error storing price for {symbol}: {str(e)}")
            raise

    async def subscribe_to_price_updates(self):
        """Subscribe to price updates channel"""
        try:
            await self.pubsub.subscribe(self.price_channel)
            logger.info(f"Subscribed to {self.price_channel}")
        except Exception as e:
            logger.error(f"Error subscribing to price updates: {str(e)}")
            raise

    async def get_price_updates(self):
        """Get price updates from subscription"""
        try:
            while True:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    logger.debug(f"Received price update: {data}")
                    yield data
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error getting price updates: {str(e)}")
            raise

    async def get_previous_price(self, symbol: str, window: int = None) -> dict:
        """Get previous price data for change calculation"""
        key = self.price_key_format.format(symbol=symbol)
        window = window or self.week_seconds
        current_time = int(time.time())
        previous_time = current_time - window
        
        try:
            logger.debug(f"Fetching previous price for {symbol} from {previous_time} to {current_time}")
            
            prices = await self.redis_client.zrangebyscore(
                key, 
                previous_time, 
                current_time, 
                start=0, 
                num=1
            )
            
            if prices:
                price_data = eval(prices[0])  # Note: Consider using json.loads instead of eval for safety
                logger.info(f"Found previous price for {symbol}: {json.dumps(price_data)}")
                return price_data
            else:
                logger.warning(f"No previous price found for {symbol} in the last {window} seconds")
                return None
                
        except Exception as e:
            logger.error(f"Redis error fetching previous price for {symbol}: {str(e)}")
            raise

    async def close(self):
        """Close Redis connections"""
        try:
            await self.pubsub.unsubscribe(self.price_channel)
            await self.redis_client.close()
            logger.info("Closed Redis connections")
        except Exception as e:
            logger.error(f"Error closing Redis connections: {str(e)}")
            raise
