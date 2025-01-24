from django.urls import path
from .consumers import MarketConsumer

websocket_urlpatterns = [
    path('markets/ws/', MarketConsumer.as_asgi()),
]