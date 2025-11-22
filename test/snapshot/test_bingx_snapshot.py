import pytest

from snapshot.bingx import BingxSnapshotAsync


class FakeBingxExchange:
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
                    "asset": "USDT",
                    "balance": "120.5",
                    "availableMargin": "100.0",
                    "equity": "120.5",
                    "realisedProfit": "5.5",
                }
            ]
        }


@pytest.mark.asyncio
async def test_bingx_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.bingx.BingxExchangeAsync", FakeBingxExchange)
    portfolio = portfolio_factory("bingx")
    snapshot = BingxSnapshotAsync(portfolio, snapshot_time, interval=5, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.portfolio == portfolio["portfolio"]
    assert summary.total_usd_value == pytest.approx(120.5)
    asset = summary.balances["USDT"]
    assert asset["available"] == pytest.approx(100.0)
    assert asset["liability"] == pytest.approx(5.5)
