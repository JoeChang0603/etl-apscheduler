import pytest
import krex
from types import SimpleNamespace
from exchange.bitmart.rest import BitmartExchangeAsync

class DummyClient:
    def __init__(self):
        self.entered = False
        self.exited = False
        self.requested_product_symbol = None

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_contract_assets(self):
        return {"balances": []}
    
    async def get_trading_pairs_details(self):
        return {"details": []}
    
    async def get_ticker_of_a_pair(self, product_symbol):
        self.requested_product_symbol = product_symbol
        return {"pair": [], "product_symbol": product_symbol}

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def bitmart_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["bitmart"]

    async def fake_bitmart(*, api_key, api_secret, memo):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert memo == creds["memo"]
        return dummy_client

    monkeypatch.setattr("exchange.bitmart.rest.krex.bitmart", fake_bitmart)

    return BitmartExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"], "memo": creds["memo"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_bitmart_get_balance(bitmart_exchange, dummy_client):
    async with bitmart_exchange as client:
        resp = await client.get_balance()
        assert resp == {"balances": []}
    assert dummy_client.entered and dummy_client.exited

@pytest.mark.asyncio
async def test_bitmart_get_trading_pairs_details(bitmart_exchange, dummy_client):
    async with bitmart_exchange as client:
        resp = await client.get_trading_pairs_details()
        assert resp == {"details": []}
    assert dummy_client.entered and dummy_client.exited

@pytest.mark.asyncio
async def test_bitmart_get_ticker_of_a_pair(bitmart_exchange, dummy_client):
    async with bitmart_exchange as client:
        resp = await client.get_ticker_of_a_pair(product_symbol="BTC_USDT_SPOT")
        assert resp["product_symbol"] == "BTC_USDT_SPOT"
    assert dummy_client.entered and dummy_client.exited
    assert dummy_client.requested_product_symbol == "BTC_USDT_SPOT"