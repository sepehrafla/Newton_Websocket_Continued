import aiohttp
import asyncio
import logging
from typing import Dict, Any
from django.conf import settings
import time
import json
from .models import PriceHistory

logger = logging.getLogger(__name__)

class MarketDataService:
    """Service for fetching market data from Newton"""
    
    def __init__(self):
        self.session = None
        self.price_history = PriceHistory()
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
            async with session.get(settings.NEWTON_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully fetched Newton data in {time.time() - start_time:.2f}s")
                    logger.debug(f"Raw Newton data: {json.dumps(data)[:200]}...")
                    return data
                else:
                    logger.error(f"Newton API error: Status {response.status}")
                    return []
        except Exception as e:
            logger.exception(f"Error fetching Newton data: {str(e)}")
            return []

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

    async def get_market_data(self) -> dict:
        """Get formatted market data"""
        start_time = time.time()
        logger.info("Starting market data fetch and format cycle")
        
        newton_data = await self.fetch_newton_data()
        if not newton_data:
            logger.error("No data received from Newton API")
            return {}
            
        market_data = self.format_market_data(newton_data)
        response = self.get_formatted_response(market_data)
        
        logger.info(f"Completed market data cycle in {time.time() - start_time:.2f}s")
        return response

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            logger.info("Closing aiohttp session")
            await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close() 