import pytest
import krex
from datetime import datetime, timedelta
from types import SimpleNamespace
from exchange.bybit.rest import BybitExchangeAsync

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
        return {"balance": []}
    
    async def get_internal_transfer_records(self):
        return {"result": {"list": []}}

@pytest.fixture
def dummy_client():
    return DummyClient()

@pytest.fixture
def bybit_exchange(monkeypatch, exchange_keys, dummy_client):
    creds = exchange_keys["bybit"]

    async def fake_bybit(*, api_key, api_secret, preload_product_table):
        assert api_key == creds["api_key"]
        assert api_secret == creds["api_secret"]
        assert preload_product_table is False
        return dummy_client

    monkeypatch.setattr("exchange.bybit.rest.krex.bybit", fake_bybit)

    return BybitExchangeAsync(
        {"api_key": creds["api_key"], "api_secret": creds["api_secret"]},
        logger=SimpleNamespace(),
    )

@pytest.mark.asyncio
async def test_bybit_get_balance(bybit_exchange, dummy_client):
    async with bybit_exchange as client:
        resp = await client.get_balance()
        assert resp == {"balance": []}
    assert dummy_client.entered and dummy_client.exited

@pytest.mark.asyncio
async def test_bybit_get_transfer_adjustment(monkeypatch, bybit_exchange, dummy_client):
    """
    Tests get_transfer_adjustment with a 30-minute window:
    - 10 minutes ago: unified transfer in 100 → count as -100
    - 10 minutes ago: spot transfer in 50 → count as +50
    - 40 minutes ago: unified transfer in 999 → ignored (outside window)
    Net result should be -50.
    """

    fake_now = datetime(2025, 1, 1, 12, 0, 0)

    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now

    monkeypatch.setattr("exchange.bybit.rest.datetime", FakeDateTime)

    recent_ts = int((fake_now - timedelta(minutes=10)).timestamp() * 1000)
    old_ts = int((fake_now - timedelta(minutes=40)).timestamp() * 1000)

    data = {
        "result": {
            "list": [
                {
                    "timestamp": str(recent_ts),
                    "toAccountType": "Unified",
                    "amount": "100",
                },
                {
                    "timestamp": str(recent_ts),
                    "toAccountType": "SPOT",
                    "amount": "50",
                },
                {
                    "timestamp": str(old_ts),
                    "toAccountType": "Unified",
                    "amount": "999",
                },
            ]
        }
    }

    async def fake_get_internal_transfer_records():
        return data

    monkeypatch.setattr(
        dummy_client, "get_internal_transfer_records", fake_get_internal_transfer_records
    )

    async with bybit_exchange as client:
        adjustment = await client.get_transfer_adjustment(interval=30)

    # unified: -100, non-unified: +50 → -50
    assert adjustment == -50
    assert dummy_client.entered and dummy_client.exited