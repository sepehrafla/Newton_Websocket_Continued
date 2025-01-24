import json
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
import logging
from uuid import uuid4
from django.conf import settings
from django.core.cache import cache
from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)

class MarketDataConsumer(AsyncWebsocketConsumer):
    # Shared connection pool
    redis_pool = ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        max_connections=100  # Adjust based on needs
    )

    MAX_MESSAGES_PER_MINUTE = 100

    active_connections = Gauge('ws_active_connections', 'Number of active websocket connections')
    messages_total = Counter('ws_messages_total', 'Total websocket messages received')

    async def check_health(self):
        try:
            await self.redis.ping()
            return True
        except Exception:
            logger.error("Redis health check failed")
            return False

    async def connect(self):
        """Handle WebSocket connection"""
        self.client_id = str(uuid4())
        self.subscribed_channels = set()  # Track subscribed channels
        logger.info(f"New client connecting: {self.client_id}")
        
        try:
            self.redis = Redis(connection_pool=self.redis_pool)
            self.pubsub = self.redis.pubsub()
            
            # Subscribe to market rates channel
            await self.pubsub.subscribe('market_rates')
            self.subscribed_channels.add('market_rates')  # Track subscription
            await self.accept()
            
            # Send initial cached data if available
            cached_data = await self.redis.get('market_data_cache')
            if cached_data:
                await self.send(text_data=cached_data)
                logger.debug(f"Sent initial cached data to client {self.client_id}")
            
            # Start listening for messages
            await self.listen_for_updates()
            
            if not await self.check_health():
                await self.close(code=1013)  # Try another server
                return
            
            self.active_connections.inc()
            
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
            # Unsubscribe from all channels
            for channel in self.subscribed_channels.copy():
                await self.pubsub.unsubscribe(channel)
                self.subscribed_channels.remove(channel)
            await self.redis.close()
            logger.info(f"Client {self.client_id} disconnected cleanly")
            self.active_connections.dec()
        except Exception as e:
            logger.error(f"Error during client {self.client_id} disconnect: {str(e)}")

    async def receive(self, text_data):
        """Handle incoming WebSocket messages"""
        self.messages_total.inc()
        rate_key = f"rate_limit:{self.client_id}"
        current = await cache.get(rate_key, 0)
        
        if current > self.MAX_MESSAGES_PER_MINUTE:
            await self.send(text_data=json.dumps({
                "event": "error",
                "message": "Rate limit exceeded"
            }))
            return
            
        await cache.incr(rate_key)
        if current == 0:
            await cache.expire(rate_key, 60)  # Reset after 1 minute
        
        try:
            logger.debug(f"Received message from client {self.client_id}: {text_data}")
            message = json.loads(text_data)
            
            if message.get("event") == "subscribe":
                channel = message.get("channel")
                if channel == "price_updates":
                    await self.handle_price_updates_subscription()
                else:
                    logger.warning(f"Client {self.client_id} requested unknown channel: {channel}")
            elif message.get("event") == "unsubscribe":
                channel = message.get("channel")
                await self.handle_unsubscribe(channel)
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

    async def handle_unsubscribe(self, channel: str):
        """Handle unsubscription from a channel"""
        try:
            if channel in self.subscribed_channels:
                await self.pubsub.unsubscribe(channel)
                self.subscribed_channels.remove(channel)
                await self.send(text_data=json.dumps({
                    "event": "unsubscribed",
                    "channel": channel
                }))
                logger.info(f"Client {self.client_id} unsubscribed from {channel}")
            else:
                await self.send(text_data=json.dumps({
                    "event": "error",
                    "message": f"Not subscribed to channel: {channel}"
                }))
        except Exception as e:
            logger.error(f"Error unsubscribing from {channel}: {str(e)}")
            await self.send(text_data=json.dumps({
                "event": "error",
                "message": f"Failed to unsubscribe from {channel}"
            }))

    async def handle_price_updates_subscription(self):
        """Handle subscription to individual price updates"""
        try:
            await self.pubsub.subscribe('price_updates')
            self.subscribed_channels.add('price_updates')  # Track subscription
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
