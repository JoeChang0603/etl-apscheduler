from datetime import timedelta

from exchange.bybit import BybitExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from snapshot.base import SnapshotBase
from utils.model_parser import model_parser
from utils.misc import datetime_to_str


class BybitSnapshotAsync(SnapshotBase):
    """Produce account summaries for Bybit portfolios."""

    def __init__(self, portfolio, current_time, interval, logger):
        """Store Bybit snapshot context and configuration.

        :param portfolio: Portfolio metadata with Bybit credentials.
        :param current_time: Datetime representing the snapshot timestamp.
        :param interval: Interval in minutes used for transfer adjustments.
        :param logger: Logger used for diagnostics.
        """
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        """Collect balances and return an ``AccountSummary`` for Bybit.

        :return: ``AccountSummary`` populated with Bybit wallet data.
        """
        async with BybitExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp["data"][0]  
            balances = {}

            for asset in balance["coin"]:
                tmp = model_parser(
                    AssetBalance(
                        total=asset["walletBalance"],
                        available=float(asset["equity"]),
                        notional=float(asset["usdValue"]),
                        liability=float(asset["cumRealisedPnl"]),
                        interest=float(asset["accruedInterest"])
                        if asset["accruedInterest"] != ""
                        else 0.0,
                    )
                )
                balances[asset["coin"]] = tmp

            return AccountSummary(
                portfolio=self.portfolio["portfolio"],
                exchange=self.portfolio["exchange"],
                total_usd_value=float(balance["totalEquity"]),
                balances=balances,
                transfer_adjustment=float(self.exchange.get_transfer_adjustment(self.interval)),
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time + timedelta(hours=8))
            )
