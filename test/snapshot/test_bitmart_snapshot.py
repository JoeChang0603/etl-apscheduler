import pytest

from snapshot.bitmart import BitmartSnapshotAsync


class FakeBitmartExchange:
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
                {"currency": "BTC", "available_balance": "0.2", "frozen_balance": "0.1"},
                {"currency": "USDT", "available_balance": "100", "frozen_balance": "0"},
            ]
        }

    async def get_trading_pairs_details(self):
        return {
            "data": {
                "symbols": [
                    {"base_currency": "BTC", "symbol": "BTC_USDT", "trade_status": "trading"}
                ]
            }
        }

    async def get_ticker_of_a_pair(self, product_symbol):
        assert product_symbol == "BTC-USDT-SPOT"
        return {"data": {"last": "20000"}}


@pytest.mark.asyncio
async def test_bitmart_snapshot_account_summary(monkeypatch, portfolio_factory, snapshot_time, dummy_logger):
    monkeypatch.setattr("snapshot.bitmart.BitmartExchangeAsync", FakeBitmartExchange)
    portfolio = portfolio_factory("bitmart")
    snapshot = BitmartSnapshotAsync(portfolio, snapshot_time, interval=5, logger=dummy_logger)

    summary = await snapshot.snapshot_account_summary()

    assert summary.total_usd_value == pytest.approx(4100.0)
    btc = summary.balances["BTC"]
    assert btc["notional"] == pytest.approx(6000.0)
    usdt = summary.balances["USDT"]
    assert usdt["total"] == pytest.approx(100.0)
