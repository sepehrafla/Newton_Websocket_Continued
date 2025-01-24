import json
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from .services import MarketDataService
import logging
from uuid import uuid4
import time

logger = logging.getLogger(__name__)

class MarketConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.client_id = str(uuid4())
        logger.info(f"New client connecting: {self.client_id}")
        
        try:
            self.market_service = MarketDataService()
            await self.accept()
            logger.info(f"Client {self.client_id} connected successfully")
        except Exception as e:
            logger.error(f"Error during client {self.client_id} connection: {str(e)}")
            raise

    async def disconnect(self, close_code):
        logger.info(f"Client {self.client_id} disconnecting with code: {close_code}")
        try:
            await self.market_service.close()
            logger.info(f"Client {self.client_id} disconnected cleanly")
        except Exception as e:
            logger.error(f"Error during client {self.client_id} disconnect: {str(e)}")

    async def receive(self, text_data):
        try:
            logger.debug(f"Received message from client {self.client_id}: {text_data}")
            message = json.loads(text_data)
            
            if message.get("event") == "subscribe" and message.get("channel") == "rates":
                logger.info(f"Client {self.client_id} subscribing to rates channel")
                await self.handle_market_data_subscription()
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
        update_count = 0
        
        while True:
            cycle_start = time.time()
            try:
                response = await self.market_service.get_market_data()
                if response:
                    await self.send(text_data=json.dumps(response))
                    update_count += 1
                    logger.debug(
                        f"Sent update #{update_count} to client {self.client_id} "
                        f"in {time.time() - cycle_start:.3f}s"
                    )
                else:
                    logger.warning(f"No data available for client {self.client_id}")
            except Exception as e:
                logger.error(f"Error sending update to client {self.client_id}: {str(e)}")
            await asyncio.sleep(1)