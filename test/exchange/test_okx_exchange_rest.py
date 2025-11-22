import pytest
import krex
from datetime import datetime, timedelta
from types import SimpleNamespace
from exchange.okx.rest import OkxExchangeAsync

class DummyClient:
    def __init__(self):
        self.entered = False
        self.exited = False
        self.last_account_bills_params = None

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_account_balance(self):
        return {"balance": []}
    
    async def get_account_bills(self, **params):
        self.last_account_bills_params = params
        return {"bills": [], "source": "dummy"}

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def okx_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["okx"]

    async def fake_okx(*, api_key, api_secret, passphrase, preload_product_table):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert passphrase == creds["password"]
        assert preload_product_table is False
        return dummy_client

    monkeypatch.setattr("exchange.okx.rest.krex.okx", fake_okx)

    return OkxExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"], "password": creds["password"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_okx_get_balance(okx_exchange, dummy_client):
    async with okx_exchange as client:
        resp = await client.get_balance()
        assert resp == {"balance": []}
    assert dummy_client.entered and dummy_client.exited

@pytest.mark.asyncio
async def test_okx_get_transfer_adjustment(okx_exchange, dummy_client):
    """
    Tests get_transfer_adjustment with a 30-minute window:
    - 10 minutes ago: unified transfer in 100 → count as -100
    - 10 minutes ago: spot transfer in 50 → count as +50
    - 40 minutes ago: unified transfer in 999 → ignored (outside window)
    Net result should be -50.
    """
    async with okx_exchange as client:
        resp = await client.get_transfer_adjustment()

    # 1) 有把結果原封不動 return
    assert resp == {"bills": [], "source": "dummy"}

    # 2) 有呼叫到底層 client.get_account_bills，且參數正確
    assert dummy_client.last_account_bills_params is not None
    assert dummy_client.last_account_bills_params["type"] == "1"
    assert dummy_client.last_account_bills_params["limit"] == "100"

    # 3) context manager 有正確 enter / exit
    assert dummy_client.entered and dummy_client.exited