import pytest
import krex
from types import SimpleNamespace
from exchange.zoomex.rest import ZoomexExchangeAsync

class DummyClient:
    def __init__(self):
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_wallet_balance(self):
        return {"balances": []}

@pytest.fixture 
def dummy_client():
    return DummyClient()

@pytest.fixture
def zoomex_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["zoomex"]

    async def fake_zoomex(*, api_key, api_secret, preload_product_table):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert preload_product_table is False
        return dummy_client

    monkeypatch.setattr("exchange.zoomex.rest.krex.zoomex", fake_zoomex)

    return ZoomexExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_zoomex_get_balance(zoomex_exchange, dummy_client):
    async with zoomex_exchange as client:
        resp = await client.get_balance()
        assert resp == {"balances": []}
    assert dummy_client.entered and dummy_client.exited