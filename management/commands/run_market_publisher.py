from django.core.management.base import BaseCommand
from markets.consumers import MarketDataService
import asyncio

class Command(BaseCommand):
    help = 'Runs the market publisher service'

    def handle(self, *args, **options):
        async def main():
            async with MarketDataService() as service:
                await service.fetch_and_publish_market_data()

        asyncio.run(main()) 