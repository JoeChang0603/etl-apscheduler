# tests/ws_probe.py
import krex.async_support as krex
import asyncio

from src.model.account_summary import AccountSummary, AssetBalance
from src.utils.model_parser import model_parser

async def main():
    client = await krex.bitmart(
        api_key="542d712d60aa80aa5beac6f357c851573668da4f",
        api_secret="17ce5cfdb1e4392e1cd2cfdcc91271e26a0eac581ed90df6f08462e3665baf49",
        memo = "trade",
        preload_product_table=True,
    )

    result = await client.get_contract_assets()
    balance = result['data']
    balances = {}
    total_usd = 0.0

    print(balance)

    pairs_data = await client.get_trading_pairs_details()
    

    for asset in balance:
        currency = asset["currency"]
        pairs = next(
            (d for d in pairs_data["data"]["symbols"]
            if d.get("base_currency") == currency and d.get("trade_status") == "trading"),
            None
        )

        if pairs:
            pair = pairs['symbol'].split("_")
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
    result = AccountSummary(
                    portfolio="",#"self.portfolio["portfolio"]",
                    exchange="",#self.portfolio["exchange"],
                    total_usd_value=total_usd,
                    balances=balances,
                    transfer_adjustment= 0,  # 先設為 0 帶krex補上fucction後更新
                    current_time="",#self.current_time,
                    tw_time=""#datetime_to_str(self.current_time + timedelta(hours=8))
                )

    
    print(result)

if __name__ == "__main__":
    asyncio.run(main())