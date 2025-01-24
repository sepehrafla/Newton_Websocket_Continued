import json
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from redis.asyncio import Redis
import logging
from uuid import uuid4
from django.conf import settings

logger = logging.getLogger(__name__)

class MarketConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.client_id = str(uuid4())
        logger.info(f"New client connecting: {self.client_id}")
        
        try:
            # Connect to Redis
            self.redis = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.pubsub = self.redis.pubsub()
            await self.accept()
            logger.info(f"Client {self.client_id} connected successfully")
        except Exception as e:
            logger.error(f"Error during client {self.client_id} connection: {str(e)}")
            raise

    async def disconnect(self, close_code):
        logger.info(f"Client {self.client_id} disconnecting with code: {close_code}")
        try:
            await self.pubsub.unsubscribe('market_rates')
            await self.pubsub.unsubscribe('price_updates')  # Unsubscribe from price updates too
            await self.redis.close()
            logger.info(f"Client {self.client_id} disconnected cleanly")
        except Exception as e:
            logger.error(f"Error during client {self.client_id} disconnect: {str(e)}")

    async def receive(self, text_data):
        try:
            logger.debug(f"Received message from client {self.client_id}: {text_data}")
            message = json.loads(text_data)
            
            if message.get("event") == "subscribe":
                channel = message.get("channel")
                if channel == "rates":
                    await self.handle_market_data_subscription()
                elif channel == "price_updates":
                    await self.handle_price_updates_subscription()
            else:
                logger.warning(f"Client {self.client_id} sent invalid message: {message}")
                await self.send(text_data=json.dumps({
                    "event": "error",
                    "message": "Invalid message format"
                }))
                
        except json.JSONDecodeError:
            logger.error(f"Client {self.client_id} sent invalid JSON: {text_data}")
            await self.send(text_data=json.dumps({
                "event": "error",
                "message": "Invalid JSON format"
            }))
        except Exception as e:
            logger.exception(f"Error processing message from client {self.client_id}: {str(e)}")
            await self.send(text_data=json.dumps({
                "event": "error",
                "message": "Internal server error"
            }))

    async def handle_market_data_subscription(self):
        logger.info(f"Starting market data updates for client {self.client_id}")
        
        try:
            # Subscribe to the market_rates channel
            await self.pubsub.subscribe('market_rates')
            logger.info(f"Subscribed to market_rates channel")
            
            # Get initial data from cache
            cached_data = await self.redis.get('market_data_cache')
            if cached_data:
                data = json.loads(cached_data)
                await self.send(text_data=json.dumps(data))
                logger.debug(f"Sent initial cached data to client {self.client_id}")
            else:
                logger.warning("No cached data available")
            
            # Listen for updates
            while True:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                logger.debug(f"Received message from Redis: {message}")
                if message and message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        await self.send(text_data=json.dumps(data))
                        logger.debug(f"Sent market update to client {self.client_id}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode market data: {e}")
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in subscription handler for client {self.client_id}: {str(e)}")
            logger.exception(e)
            await self.pubsub.unsubscribe('market_rates')

    async def handle_price_updates_subscription(self):
        """Handle subscription to individual price updates"""
        try:
            await self.pubsub.subscribe('price_updates')
            logger.info(f"Client {self.client_id} subscribed to price updates")
            
            while True:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    await self.send(text_data=json.dumps(data))
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in price updates handler: {str(e)}")
            await self.pubsub.unsubscribe('price_updates')