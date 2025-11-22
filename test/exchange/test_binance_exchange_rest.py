import pytest
import krex
from types import SimpleNamespace
from exchange.binance.rest import BinanceExchangeAsync

class DummyClient:
    def __init__(self):
        self.entered = False
        self.exited = False
        self.requested_market = None

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_account_balance(self, market_type="spot"):
        self.requested_market = market_type
        return {"balances": [], "market_type": market_type}
    
    async def get_spot_price(self):
        return {"price": []}

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def binance_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["binance"]

    async def fake_binance(*, api_key, api_secret, preload_product_table):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert preload_product_table is False
        return dummy_client

    monkeypatch.setattr("exchange.binance.rest.krex.binance", fake_binance)

    return BinanceExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_binance_get_balance(binance_exchange, dummy_client):
    async with binance_exchange as client:
        resp = await client.get_balance(market_type="margin")
        assert resp["market_type"] == "margin"
    assert dummy_client.entered and dummy_client.exited
    assert dummy_client.requested_market == "margin"

@pytest.mark.asyncio
async def test_binance_get_spot_price(binance_exchange, dummy_client):
    async with binance_exchange as client:
        resp = await client.get_spot_price()
        assert resp == {"price": []}
    assert dummy_client.entered and dummy_client.exited