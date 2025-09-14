import krex.async_support as krex
from datetime import datetime, timedelta
from exchange.base import ExchangeBase
from utils.logger_factory import log_exception 


class BingxExchangeAsync(ExchangeBase):
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.client = None
        self.logger = logger
    
    async def __aenter__(self):
        self.client = await krex.bingx(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc, tb)

    async def get_balance(self):
        return await self.client.get_account_balance()
    
    # Wait for Development
    # async def get_transfer_adjustment(self, interval):
    #     data = await self.client.get_internal_transfer_records()
    #     try:
    #         transactions = data["result"]["list"]
    #         adjustment = 0
    #         time_before = int((datetime.now() - timedelta(minutes=interval)).timestamp() * 1000)
    #         for transaction in transactions:
    #             if int(transaction["timestamp"]) > time_before:
    #                 if transaction["toAccountType"].lower() == "unified":
    #                     adjustment -= float(transaction["amount"])
    #                 else:
    #                     adjustment += float(transaction["amount"])
    #     except Exception as e:
    #         log_exception(self.logger, e, context="Bybit Exchange")
    #         return 0
    #     return adjustment

