from datetime import timedelta

from snapshot.base import SnapshotBase
from exchange.binance import BinanceExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from utils.model_parser import model_parser
from utils.misc import datetime_to_str


class BinanceSnapshotAsync(SnapshotBase):
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        async with BinanceExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balance = resp.get("balances", [])
            price_map = {p["symbol"]: float(p["price"]) for p in await client.get_spot_price()}
            balances = {}

            total_usd = 0
            results = []

            for b in balance:
                asset = b["asset"]
                amount = float(b["free"]) + float(b["locked"])
                if amount == 0:
                    continue
                if asset in ["USDT", "BUSD", "USDC"]:
                    price = 1.0
                else:
                    # 優先找 USDT 對，找不到就跳過
                    pair = asset + "USDT"
                    price = price_map.get(pair)
                    if price is None:
                        continue
                usd_value = amount * price
                total_usd += usd_value
                results.append({
                    "asset": asset,
                    "amount": round(amount, 6),
                    "price": round(price, 4),
                    "usd_value": round(usd_value, 2)
                })

            for asset in results:
                tmp = model_parser(
                    AssetBalance(
                        total=asset["amount"],
                        available=float(asset["amount"]),
                        notional=float(asset["usd_value"]),
                        liability=0.0,
                        interest=0.0
                    )
                )
                balances[asset["asset"]] = tmp

            return AccountSummary(
                portfolio=self.portfolio["portfolio"],
                exchange=self.portfolio["exchange"],
                total_usd_value= total_usd,
                balances=balances,
                transfer_adjustment=0,       # Disable, wait for development
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time - timedelta(hours=8))
            )

