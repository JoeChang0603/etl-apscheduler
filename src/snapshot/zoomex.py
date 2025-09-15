from snapshot.base import SnapshotBase
from exchange.zoomex import ZoomexExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from utils.model_parser import model_parser
from utils.misc import datetime_to_str
from utils.logger_factory import log_exception

from datetime import datetime, timedelta


class ZoomexSnapshotAsync(SnapshotBase):
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        async with ZoomexExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp["result"]["list"][0]

            STABLES = {"USD", "USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "USDe", "USDD"}

            balances = {}
            total_usd = 0.0
            missing_prices = []  # 若有非穩定幣且沒有 usdValue，可在後面用行情補價

            for asset in balance.get("coin", []):
                coin = asset.get("coin")

                total = float(asset.get("walletBalance"))
                available = float(asset.get("equity"))
                liability = float(asset.get("cumRealisedPnl"))
                interest = float(asset.get("accruedInterest") or 0)

                if asset.get("usdValue") not in (None, "", "null"):
                    usd_notional = float(asset.get("usdValue"))
                elif coin in STABLES:
                    usd_notional = available
                else:
                    usd_notional = 0.0
                    missing_prices.append((coin, available))  # 需要行情時在這裡補

                tmp = model_parser(
                    AssetBalance(
                        total=total,
                        available=available,
                        notional=usd_notional,
                        liability=liability,
                        interest=interest,
                    )
                )
                balances[coin] = tmp
                total_usd += usd_notional

            return AccountSummary(
                portfolio=self.portfolio["portfolio"],
                exchange=self.portfolio["exchange"],
                total_usd_value=float(total_usd),
                balances=balances,
                transfer_adjustment= 0, 
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time + timedelta(hours=8))
            )
