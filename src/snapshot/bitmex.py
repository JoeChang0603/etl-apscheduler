from datetime import timedelta

from snapshot.base import SnapshotBase
from exchange.bitmex import BitmexExchangeAsync
from model.account_summary import AccountSummary, AssetBalance
from utils.model_parser import model_parser
from utils.misc import datetime_to_str



class BitmexSnapshotAsync(SnapshotBase):
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    async def snapshot_account_summary(self):
        async with BitmexExchangeAsync(self.portfolio, self.logger) as client:
            resp = await client.get_balance()
            balances = {}
            total_usd = 0.0
            
            for asset in resp:
                currency = asset["currency"]
                raw_amount = asset["marginBalance"] if asset["marginBalance"] != 0 else 0

                # 精度處理
                if currency.upper() in ["XBT", "BTC", "WBTC"]:
                    amount = raw_amount / 1e8
                else:
                    amount = raw_amount / 1e6

                tmp = model_parser(
                    AssetBalance(
                        total=amount,
                        available=amount,      
                        notional=amount,          
                        liability=0.0,       
                        interest=0.0            
                    )
                )
                balances[currency] = tmp

                # 計算 USD 總值
                if currency.upper() in ["USDT", "USD", "USDC"]:
                    total_usd += amount
                else:
                    info = await client.get_instrument_info()
                    for item in info:
                        if item.get("typ") == "IFXXXP" and item.get("symbol").upper() == f"{currency.upper()}_USDT":
                            total_usd += amount * item.get("lastPrice")
                            break

            return AccountSummary(
                portfolio=self.portfolio['portfolio'],
                exchange=self.portfolio["exchange"],
                total_usd_value=total_usd,
                balances=balances,
                transfer_adjustment=0,      # 先設為 0 帶krex補上fucction後更新
                current_time=self.current_time,
                tw_time=datetime_to_str(self.current_time - timedelta(hours=8))
            )

