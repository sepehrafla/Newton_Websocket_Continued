from django.core.management.base import BaseCommand
import asyncio
from markets.services import MarketDataService
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Runs the market data publisher service'

    async def handle_async(self, *args, **options):
        service = MarketDataService()
        try:
            await service.fetch_and_publish_market_data()
        finally:
            await service.close()

    def handle(self, *args, **options):
        asyncio.run(self.handle_async(*args, **options)) 