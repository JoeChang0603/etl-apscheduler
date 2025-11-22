import pytest

from snapshot.zoomex import ZoomexSnapshotAsync


class FakeZoomexExchange:
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.logger = logger

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_balance(self):
        return {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": "200",
                                "equity": "200",
                                "usdValue": "200",
                                "cumRealisedPnl": "10",
                                "accruedInterest": "0.5",
                            },
                            {
                                "coin": "BTC",
                                "walletBalance": "0.2",
                                "equity": "0.2",
                                "usdValue": "",
                                "cumRealisedPnl": "0",
                                "accruedInterest": "",
                            },
                        ]
                    }
                ]
            }
        }


@pytest.mark.asyncio
async def test_zoomex_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.zoomex.ZoomexExchangeAsync", FakeZoomexExchange)
    portfolio = portfolio_factory("zoomex")
    snapshot = ZoomexSnapshotAsync(portfolio, snapshot_time, interval=5, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.total_usd_value == pytest.approx(200.0)
    assert summary.balances["USDT"]["interest"] == pytest.approx(0.5)
    assert "BTC" in summary.balances
