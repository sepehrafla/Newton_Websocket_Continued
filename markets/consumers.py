import json
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from redis.asyncio import Redis
import logging
from uuid import uuid4
from django.conf import settings

logger = logging.getLogger(__name__)

class MarketDataConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        """Handle WebSocket connection"""
        self.client_id = str(uuid4())
        logger.info(f"New client connecting: {self.client_id}")
        
        try:
            self.redis = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            self.pubsub = self.redis.pubsub()
            
            # Subscribe to market rates channel
            await self.pubsub.subscribe('market_rates')
            await self.accept()
            
            # Send initial cached data if available
            cached_data = await self.redis.get('market_data_cache')
            if cached_data:
                await self.send(text_data=cached_data)
                logger.debug(f"Sent initial cached data to client {self.client_id}")
            
            # Start listening for messages
            await self.listen_for_updates()
            
        except Exception as e:
            logger.error(f"Error during client {self.client_id} connection: {str(e)}")
            raise

    async def listen_for_updates(self):
        """Listen for Redis messages and forward to WebSocket"""
        try:
            while True:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                if message and message['type'] == 'message':
                    # Forward message to WebSocket client
                    await self.send(text_data=message['data'])
                    logger.debug(f"Forwarded market update to client {self.client_id}")
                await asyncio.sleep(0.1)  # Prevent busy waiting
        except Exception as e:
            logger.error(f"Error in message listener for client {self.client_id}: {str(e)}")

    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        logger.info(f"Client {self.client_id} disconnecting with code: {close_code}")
        try:
            await self.pubsub.unsubscribe()
            await self.redis.close()
            logger.info(f"Client {self.client_id} disconnected cleanly")
        except Exception as e:
            logger.error(f"Error during client {self.client_id} disconnect: {str(e)}")

    async def receive(self, text_data):
        """Handle incoming WebSocket messages"""
        try:
            logger.debug(f"Received message from client {self.client_id}: {text_data}")
            message = json.loads(text_data)
            
            if message.get("event") == "subscribe":
                channel = message.get("channel")
                if channel == "price_updates":
                    await self.handle_price_updates_subscription()
                else:
                    logger.warning(f"Client {self.client_id} requested unknown channel: {channel}")
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

    async def handle_price_updates_subscription(self):
        """Handle subscription to individual price updates"""
        try:
            await self.pubsub.subscribe('price_updates')
            logger.info(f"Client {self.client_id} subscribed to price updates")
            await self.send(text_data=json.dumps({
                "event": "subscribed",
                "channel": "price_updates"
            }))
        except Exception as e:
            logger.error(f"Error in price updates subscription: {str(e)}")
            await self.send(text_data=json.dumps({
                "event": "error",
                "message": "Failed to subscribe to price updates"
            }))