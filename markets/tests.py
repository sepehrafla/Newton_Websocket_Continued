import pytest
from channels.testing import WebsocketCommunicator
from channels.routing import URLRouter
from channels.auth import AuthMiddlewareStack
import json
import asyncio
from django.test import TestCase
from django.conf import settings
from .routing import websocket_urlpatterns
from .services import MarketDataService
import os

# Mark all test classes with django_db to allow database access
pytestmark = pytest.mark.django_db

@pytest.mark.asyncio
class TestWebSocket:
    # UNIT TESTS:
    async def test_invalid_json(self):
        # Unit test - tests JSON validation in isolation
        communicator = await self.setup_communicator()
        
        await communicator.send_to(text_data="invalid json")
        response = await communicator.receive_json_from()
        
        assert response["event"] == "error"
        assert response["message"] == "Invalid JSON format"
        
        await communicator.disconnect()

    async def test_invalid_subscription_channel(self):
        # Unit test - tests channel validation logic
        communicator = await self.setup_communicator()
        
        await communicator.send_json_to({
            "event": "subscribe",
            "channel": "invalid_channel"
        })
        
        response = await communicator.receive_json_from()
        assert response["event"] == "error"
        assert "Invalid message format" in response["message"]
        
        await communicator.disconnect()

    async def test_error_handling(self):
        # Unit test - tests error handling logic
        communicator = await self.setup_communicator()
        
        # Test missing event field
        await communicator.send_json_to({"channel": "rates"})
        response = await communicator.receive_json_from()
        assert response["event"] == "error"
        
        # Test invalid event type
        await communicator.send_json_to({
            "event": "invalid",
            "channel": "rates"
        })
        response = await communicator.receive_json_from()
        assert response["event"] == "error"
        
        await communicator.disconnect()

    # INTEGRATION TESTS:
    async def test_websocket_connect(self):
        # Integration test - tests full WebSocket connection stack
        communicator = await self.setup_communicator()
        await communicator.disconnect()

    async def test_valid_subscription(self):
        # Integration test - tests full subscription flow with real data
        communicator = await self.setup_communicator()
        
        await communicator.send_json_to({
            "event": "subscribe",
            "channel": "rates"
        })
        
        # Get first response
        response = await communicator.receive_json_from(timeout=2)
        
        # Verify response structure
        assert response["channel"] == "rates"
        assert response["event"] == "data"
        assert isinstance(response["data"], dict)
        
        await communicator.disconnect()

    async def test_multiple_subscriptions(self):
        # Integration test - tests multiple client handling
        communicator1 = await self.setup_communicator()
        communicator2 = await self.setup_communicator()
        
        # Subscribe both clients
        for communicator in [communicator1, communicator2]:
            await communicator.send_json_to({
                "event": "subscribe",
                "channel": "rates"
            })
        
        # Verify both receive data
        for communicator in [communicator1, communicator2]:
            response = await communicator.receive_json_from(timeout=2)
            assert response["channel"] == "rates"
            assert response["event"] == "data"
        
        await communicator1.disconnect()
        await communicator2.disconnect()
# Create your tests here.
    async def test_connection_close(self):
        # Integration test - tests connection lifecycle
        communicator = await self.setup_communicator()
        
        # Subscribe to channel
        await communicator.send_json_to({
            "event": "subscribe",
            "channel": "rates"
        })
        
        # Verify subscription works
        response = await communicator.receive_json_from(timeout=2)
        assert response["channel"] == "rates"
        
        # Close connection
        await communicator.disconnect()

    async def test_market_data_service(self):
        # Integration test - tests MarketDataService with real API
        service = MarketDataService()
        
        try:
            # Test data fetching
            data = await service.fetch_newton_data()
            assert isinstance(data, list)
            
            # Test data formatting
            formatted = service.format_market_data(data)
            assert isinstance(formatted, dict)
            
            # Test response formatting
            response = service.get_formatted_response(formatted)
            assert response["channel"] == "rates"
            assert response["event"] == "data"
            assert isinstance(response["data"], dict)
        finally:
            await service.close()

    async def test_rate_updates(self):
        # Integration test - tests real-time data flow
        communicator = await self.setup_communicator()
        
        await communicator.send_json_to({
            "event": "subscribe",
            "channel": "rates"
        })
        
        # Get multiple updates
        responses = []
        for _ in range(2):  # Reduced to 2 updates to speed up tests
            response = await communicator.receive_json_from(timeout=2)
            responses.append(response)
            await asyncio.sleep(0.1)  # Reduced sleep time
        
        # Verify we got responses
        assert len(responses) == 2
        assert all(r["channel"] == "rates" for r in responses)
        
        await communicator.disconnect()

    async def test_supported_assets(self):
        # Integration test - tests with real Newton API data
        service = MarketDataService()
        try:
            data = await service.fetch_newton_data()
            formatted = service.format_market_data(data)
            
            # Verify some supported assets are present (not all might be available)
            assert len(formatted) > 0
            # Verify format of at least one asset
            first_asset = list(formatted.values())[0]
            assert "symbol" in first_asset
            assert "timestamp" in first_asset
            assert "bid" in first_asset
            assert "ask" in first_asset
            assert "spot" in first_asset
            assert "change" in first_asset
        finally:
            await service.close()

    # HELPER METHOD:
    async def setup_communicator(self):
        # Helper method for test setup
        application = AuthMiddlewareStack(URLRouter(websocket_urlpatterns))
        communicator = WebsocketCommunicator(application, "/markets/ws/")
        connected, _ = await communicator.connect()
        assert connected
        return communicator