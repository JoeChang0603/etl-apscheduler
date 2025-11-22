from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from snapshot.okx import OkxSnapshotAsync


class FakeOkxExchange:
    def __init__(self, portfolio, logger, balance_payload=None, transfer_payload=None):
        self.portfolio = portfolio
        self.logger = logger
        self._balance_payload = balance_payload
        self._transfer_payload = transfer_payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_balance(self):
        return self._balance_payload

    async def get_transfer_adjustment(self):
        return self._transfer_payload


@pytest.mark.asyncio
async def test_okx_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    balance_payload = {
        "data": [
            {
                "totalEq": "123.45",
                "details": [
                    {"ccy": "USDT", "eq": "100", "availEq": "90", "eqUsd": "100", "liab": "", "interest": ""},
                    {"ccy": "BTC", "eq": "0.01", "availEq": "0.005", "eqUsd": "300", "liab": "1", "interest": "0.1"},
                ],
            }
        ]
    }
    fake_exchange = FakeOkxExchange(None, None, balance_payload=balance_payload)
    monkeypatch.setattr("snapshot.okx.OkxExchangeAsync", lambda *args, **kwargs: fake_exchange)
    monkeypatch.setattr(OkxSnapshotAsync, "get_transfer_adjustment", AsyncMock(return_value=5.0))

    portfolio = portfolio_factory("okx")
    snapshot = OkxSnapshotAsync(portfolio, snapshot_time, interval=30, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.total_usd_value == pytest.approx(123.45)
    assert summary.transfer_adjustment == pytest.approx(5.0)
    assert summary.balances["BTC"]["interest"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_okx_get_transfer_adjustment(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    fake_now = datetime(2025, 1, 1, 12, 0, 0)

    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fake_now

    monkeypatch.setattr("snapshot.okx.datetime", FakeDateTime)

    recent_ts = int((fake_now - timedelta(minutes=10)).timestamp() * 1000)
    old_ts = int((fake_now - timedelta(minutes=60)).timestamp() * 1000)

    transfer_payload = {
        "data": [
            {"ts": str(recent_ts), "to": "18", "balChg": "-10"},  # subtract 10
            {"ts": str(recent_ts), "to": "6", "balChg": "7"},    # add 7
            {"ts": str(old_ts), "to": "6", "balChg": "999"},     # ignored (old)
        ]
    }

    fake_exchange = FakeOkxExchange(None, None, transfer_payload=transfer_payload)
    monkeypatch.setattr("snapshot.okx.OkxExchangeAsync", lambda *args, **kwargs: fake_exchange)

    portfolio = portfolio_factory("okx")
    snapshot = OkxSnapshotAsync(portfolio, snapshot_time, interval=30, logger=dummy_logger)

    adjustment = await snapshot.get_transfer_adjustment(interval=30)

    # -10 (to 18) + 7 (to 6) = -3
    assert adjustment == pytest.approx(-3.0)
