from datetime import datetime, timedelta

from snapshot.base import SnapshotBase
from exchange.okx import OkxExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from utils.model_parser import model_parser
from utils.misc import datetime_to_str
from utils.logger_factory import log_exception




class OkxSnapshotAsync(SnapshotBase):
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        async with OkxExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp["data"][0]  

            balances = {}
            for asset in balance["details"]:
                tmp = model_parser(
                    AssetBalance(
                        total=float(asset["eq"]) if asset["eq"] != "" else 0,
                        available=float(asset["availEq"]) if asset["availEq"] != "" else 0,
                        notional= float(asset["eqUsd"]) if asset["eqUsd"] != "" else 0,
                        liability=float(asset["liab"]) if asset["liab"] != "" else 0,
                        interest=float(asset["interest"]) if asset["interest"] != "" else 0,
                    )
                )
                balances[asset["ccy"]] = tmp

            return AccountSummary(
                portfolio=self.portfolio["portfolio"],
                exchange=self.portfolio["exchange"],
                total_usd_value=float(balance["totalEq"]),
                balances=balances,
                transfer_adjustment=float(await self.get_transfer_adjustment(0.5)),
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time + timedelta(hours=8))
            )

    async def get_transfer_adjustment(self, interval):
        async with OkxExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_transfer_adjustment()
            self.logger.info(resp)
         
            transactions = resp.get("data", "")
            adjustment = 0
            time_before = int(
                (datetime.now() - timedelta(minutes=interval)).timestamp() * 1000
            )

            for transaction in transactions:
                if int(transaction["ts"]) > time_before:
                    # transaction["to"] : {"6": "funding", "18": "trading"} 
                    if transaction["to"] == "18":   
                        adjustment -= abs(float(transaction["balChg"]))
                    elif transaction["to"] == "6":
                        adjustment += abs(float(transaction["balChg"]))
            return adjustment
