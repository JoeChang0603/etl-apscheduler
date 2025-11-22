from types import SimpleNamespace

import pytest

from snapshot.bybit import BybitSnapshotAsync


class FakeBybitExchange:
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.logger = logger

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_balance(self):
        return {
            "data": [
                {
                    "coin": [
                        {
                            "coin": "USDT",
                            "walletBalance": "100",
                            "equity": "100",
                            "usdValue": "100",
                            "cumRealisedPnl": "1",
                            "accruedInterest": "0.25",
                        },
                        {
                            "coin": "BTC",
                            "walletBalance": "0.5",
                            "equity": "0.5",
                            "usdValue": "15000",
                            "cumRealisedPnl": "-2",
                            "accruedInterest": "",
                        },
                    ],
                    "totalEquity": "15100",
                }
            ]
        }


@pytest.mark.asyncio
async def test_bybit_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.bybit.BybitExchangeAsync", FakeBybitExchange)
    portfolio = portfolio_factory("bybit")
    snapshot = BybitSnapshotAsync(portfolio, snapshot_time, interval=30, logger=dummy_logger)
    snapshot.exchange = SimpleNamespace(get_transfer_adjustment=lambda interval: 25.0)

    summary = await snapshot.snapshot_account_summary()

    assert summary.total_usd_value == pytest.approx(15100.0)
    assert summary.transfer_adjustment == pytest.approx(25.0)
    btc = summary.balances["BTC"]
    assert btc["notional"] == pytest.approx(15000.0)
