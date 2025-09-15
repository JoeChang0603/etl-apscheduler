from datetime import timedelta

from snapshot.base import SnapshotBase
from exchange.bingx import BingxExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from utils.model_parser import model_parser
from utils.misc import datetime_to_str



class BingxSnapshotAsync(SnapshotBase):
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        async with BingxExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp["data"]
            balances = {}
            total_usd = 0.0

            for asset in balance:
                tmp = model_parser(
                    AssetBalance(
                        total=float(asset["balance"]),
                        available=float(asset.get("availableMargin", asset.get("equity", 0))),
                        notional=float(asset["equity"]),  
                        liability=float(asset.get("realisedProfit", 0)),
                        interest=0.0,  
                    )
                )
                balances[asset["asset"]] = tmp
                total_usd += float(asset.get("equity", 0))

            return AccountSummary(
                    portfolio=self.portfolio["portfolio"],
                    exchange=self.portfolio["exchange"],
                    total_usd_value=total_usd,
                    balances=balances,
                    transfer_adjustment= 0,  # 先設為 0 帶krex補上fucction後更新
                    current_time=self.current_time,
                    tw_time=datetime_to_str(self.current_time + timedelta(hours=8))
                )

