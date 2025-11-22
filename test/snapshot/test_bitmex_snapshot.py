import pytest

from snapshot.bitmex import BitmexSnapshotAsync


class FakeBitmexExchange:
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.logger = logger

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_balance(self):
        return [
            {"currency": "XBT", "marginBalance": 20_000_000},  # 0.2
            {"currency": "USDT", "marginBalance": 5_000_000},  # 5
        ]

    async def get_instrument_info(self):
        return [
            {"typ": "IFXXXP", "symbol": "XBT_USDT", "lastPrice": 30000},
        ]


@pytest.mark.asyncio
async def test_bitmex_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.bitmex.BitmexExchangeAsync", FakeBitmexExchange)
    portfolio = portfolio_factory("bitmex")
    snapshot = BitmexSnapshotAsync(portfolio, snapshot_time, interval=10, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.total_usd_value == pytest.approx(6005.0)
    btc = summary.balances["XBT"]
    assert btc["total"] == pytest.approx(0.2)
    assert summary.balances["USDT"]["notional"] == pytest.approx(5.0)
