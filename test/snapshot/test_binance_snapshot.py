import pytest

from snapshot.binance import BinanceSnapshotAsync


class FakeBinanceExchange:
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.logger = logger
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def get_balance(self):
        return {
            "balances": [
                {"asset": "BTC", "free": "0.1", "locked": "0.0"},
                {"asset": "USDT", "free": "200", "locked": "0"},
                {"asset": "XYZ", "free": "3", "locked": "0"},  # skipped (no price)
            ]
        }

    async def get_spot_price(self):
        return [{"symbol": "BTCUSDT", "price": "30000"}]


@pytest.mark.asyncio
async def test_binance_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.binance.BinanceExchangeAsync", FakeBinanceExchange)
    portfolio = portfolio_factory("binance")
    snapshot = BinanceSnapshotAsync(portfolio, snapshot_time, interval=15, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.portfolio == portfolio["portfolio"]
    assert summary.total_usd_value == pytest.approx(3200.0)
    assert summary.balances["BTC"]["total"] == pytest.approx(0.1)
    assert summary.balances["BTC"]["notional"] == pytest.approx(3000.0)
    assert summary.balances["USDT"]["notional"] == pytest.approx(200.0)
