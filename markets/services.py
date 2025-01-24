import aiohttp
import asyncio
import logging
from typing import Dict, Any
from django.conf import settings
import time
import json
from .models import PriceHistory
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

class MarketDataService:
    """Service for fetching market data from Newton"""
    
    def __init__(self):
        self.session = None
        self.redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )
        self.price_history = PriceHistory()
        # Create trading pairs by adding suffix
        self.supported_pairs = {f"{asset}_CAD" for asset in settings.SUPPORTED_ASSETS}
        logger.info(f"MarketDataService initialized with {len(self.supported_pairs)} supported pairs")

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            logger.debug("Creating new aiohttp session")
            self.session = aiohttp.ClientSession()
        return self.session

    async def fetch_newton_data(self) -> list:
        """Fetch market data from Newton API"""
        start_time = time.time()
        try:
            logger.debug("Fetching data from Newton API")
            session = await self.get_session()
            
            logger.debug(f"Making request to: {settings.NEWTON_API_URL}")
            
            async with session.get(settings.NEWTON_API_URL) as response:
                logger.debug(f"Got response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    # Newton returns a list directly, no need to check for errors
                    if isinstance(data, list):
                        logger.info(f"Successfully fetched Newton data in {time.time() - start_time:.2f}s")
                        logger.debug(f"Raw Newton data: {json.dumps(data)[:200]}...")
                        return data
                    else:
                        logger.error(f"Unexpected Newton API response format: {data}")
                        return []
                else:
                    response_text = await response.text()
                    logger.error(f"Newton API error: Status {response.status}, Response: {response_text}")
                    return []
        except Exception as e:
            logger.exception(f"Error fetching Newton data: {str(e)}")
            return []

    def _transform_kraken_data(self, kraken_data: dict) -> list:
        """Transform Kraken data format to match expected format"""
        transformed_data = []
        
        for pair, data in kraken_data.items():
            try:
                # Convert Kraken pair back to our format (e.g., XXBTZCAD -> BTC_CAD)
                base_asset = pair[:-3]  # Remove CAD
                if base_asset.startswith('X'):  # Kraken prefixes some pairs with X
                    base_asset = base_asset[1:]
                
                # Convert back to our symbol format
                symbol = f"{base_asset}_{settings.PAIR_SUFFIX}"
                
                transformed_data.append({
                    'symbol': symbol,
                    'bid': float(data['b'][0]),
                    'ask': float(data['a'][0]),
                    'timestamp': int(time.time() * 1000),
                    'change': float(data['p'][1]) - float(data['p'][0])  # 24h price change
                })
                logger.debug(f"Transformed {pair} to {symbol}")
            except (KeyError, IndexError, ValueError) as e:
                logger.error(f"Error transforming data for pair {pair}: {str(e)}")
                continue
        
        return transformed_data

    def format_market_data(self, newton_data: list) -> dict:
        """Format market data according to requirements"""
        start_time = time.time()
        formatted_data = {}
        processed_count = 0
        error_count = 0
        
        logger.debug(f"Starting to format {len(newton_data)} market data entries")
        
        for item in newton_data:
            symbol = item.get('symbol')
            if symbol not in self.supported_pairs:
                logger.debug(f"Skipping unsupported symbol: {symbol}")
                continue

            try:
                # Store historical data for change calculation
                self.price_history.store_price(symbol, item)
                
                bid = float(item['bid'])
                ask = float(item['ask'])
                spot = (bid + ask) / 2
                
                formatted_data[symbol] = {
                    "symbol": symbol,
                    "timestamp": item['timestamp'],
                    "bid": bid,
                    "ask": ask,
                    "spot": spot,
                    "change": float(item['change'])
                }
                processed_count += 1
                logger.debug(f"Processed {symbol}: bid={bid}, ask={ask}, spot={spot}")
                
            except (KeyError, ValueError) as e:
                error_count += 1
                logger.error(f"Error formatting {symbol} data: {str(e)}")
                continue

        logger.info(f"Formatted {processed_count} entries with {error_count} errors in {time.time() - start_time:.2f}s")
        return formatted_data

    def get_formatted_response(self, market_data: dict) -> dict:
        """Format the final WebSocket response"""
        if not market_data:
            logger.warning("No market data available for response")
            return {}
            
        response = {
            "channel": "rates",
            "event": "data",
            "data": market_data
        }
        logger.debug(f"Formatted response with {len(market_data)} symbols")
        return response

    async def fetch_and_publish_market_data(self):
        """Fetch market data and publish to Redis"""
        logger.info("Starting market data publisher service")
        publish_count = 0
        
        while True:
            try:
                # Fetch data from Newton API
                logger.info("Starting new fetch cycle...")
                newton_data = await self.fetch_newton_data()
                
                if newton_data:
                    logger.debug(f"Got Newton data with {len(newton_data)} items")
                    # Format the data
                    market_data = self.format_market_data(newton_data)
                    logger.debug(f"Formatted market data with {len(market_data)} pairs")
                    response = self.get_formatted_response(market_data)
                    
                    # Cache and publish
                    await self.redis.setex('market_data_cache', 1, json.dumps(response))
                    publish_result = await self.redis.publish('market_rates', json.dumps(response))
                    publish_count += 1
                    logger.info(f"Published update #{publish_count} to {publish_result} subscribers")
                else:
                    logger.warning("No data received from Newton API")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error("Error in fetch and publish cycle")
                logger.exception(e)
                await asyncio.sleep(1)

    async def close(self):
        """Close all connections"""
        if self.session and not self.session.closed:
            await self.session.close()
        await self.redis.close()
        await self.price_history.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # Add your service methods here 