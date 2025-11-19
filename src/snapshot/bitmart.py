from datetime import timedelta

from exchange.bitmart.rest import BitmartExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from snapshot.base import SnapshotBase
from utils.model_parser import model_parser
from utils.misc import datetime_to_str


class BitmartSnapshotAsync(SnapshotBase):
    """Produce account summaries for Bitmart portfolios."""

    def __init__(self, portfolio, current_time, interval, logger):
        """Store Bitmart snapshot context and configuration.

        :param portfolio: Portfolio metadata with Bitmart credentials.
        :param current_time: Datetime representing the snapshot timestamp.
        :param interval: Interval in minutes used for transfer adjustments.
        :param logger: Logger used for diagnostics.
        """
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        """Collect balances and return an ``AccountSummary`` for Bitmart.

        :return: ``AccountSummary`` populated with Bitmart wallet data.
        """
        async with BitmartExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp['data']
            balances = {}

            total_usd = 0.0

            pairs_data = await client.get_trading_pairs_details()

            for asset in balance:
                currency = asset["currency"]
                pair = next(
                    (d for d in pairs_data["data"]["symbols"]
                    if d.get("base_currency") == currency and d.get("trade_status") == "trading"),
                    None
                )
                
                if pair:
                    pair = pair['symbol'].split("_")
                    ticker = await client.get_ticker_of_a_pair(product_symbol=f"{pair[0]}-{pair[1]}-SPOT")
                    last_price = float(ticker.get('data',"")['last'])

                    total = float(asset['available_balance']) * last_price
                    available = float(asset["available_balance"])
                    notional = (available + float(asset["frozen_balance"])) * last_price
                else:
                    total = float(asset["available_balance"])
                    available = total
                    notional = (available + float(asset["frozen_balance"]))
                
                tmp = model_parser(
                            AssetBalance(
                                total=total,
                                available=available,
                                notional=notional,  
                                liability=0.0,
                                interest=0.0,  
                            )
                        )
                balances[asset["currency"]] = tmp
                total_usd += total

            return AccountSummary(
                portfolio=self.portfolio["portfolio"],
                exchange=self.portfolio["exchange"],
                total_usd_value=total_usd,
                balances=balances,
                transfer_adjustment=0,
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time + timedelta(hours=8))
            )
