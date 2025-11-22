import pytest
import krex
from types import SimpleNamespace
from exchange.bitmex.rest import BitmexExchangeAsync

class DummyClient:
    def __init__(self):
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_margin(self):
        return {"margin": []}
    
    async def get_instrument_info(self):
        return {"info": []}

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def bitmex_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["bitmex"]

    async def fake_bitmex(*, api_key, api_secret, preload_product_table):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert preload_product_table is False
        return dummy_client

    monkeypatch.setattr("exchange.bitmex.rest.krex.bitmex", fake_bitmex)

    return BitmexExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_bitmex_get_balance(bitmex_exchange, dummy_client):
    async with bitmex_exchange as client:
        resp = await client.get_balance()
        assert resp == {"margin": []}
    assert dummy_client.entered and dummy_client.exited

@pytest.mark.asyncio
async def test_bitmex_get_instrument_info(bitmex_exchange, dummy_client):
    async with bitmex_exchange as client:
        resp = await client.get_instrument_info()
        assert resp == {"info": []}
    assert dummy_client.entered and dummy_client.exited
