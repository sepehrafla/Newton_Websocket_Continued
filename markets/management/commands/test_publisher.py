from django.core.management.base import BaseCommand
import asyncio
from redis.asyncio import Redis
import json
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Test Redis publisher with dummy data'

    async def handle_async(self, *args, **options):
        redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )
        
        count = 0
        try:
            self.stdout.write('Starting test publisher...')
            while True:
                # Create dummy data
                test_data = {
                    "channel": "rates",
                    "event": "data",
                    "data": {
                        "BTC_CAD": {
                            "symbol": "BTC_CAD",
                            "price": 50000 + count,
                            "timestamp": count
                        }
                    }
                }
                
                # Publish to Redis
                result = await redis.publish('market_rates', json.dumps(test_data))
                self.stdout.write(f'Published test data #{count} to {result} subscribers')
                count += 1
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            self.stdout.write('Stopping test publisher...')
        finally:
            await redis.close()

    def handle(self, *args, **options):
        asyncio.run(self.handle_async(*args, **options)) 