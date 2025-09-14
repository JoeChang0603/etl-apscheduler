import krex.async_support as krex
from datetime import datetime, timedelta
from exchange.base import ExchangeBase
from utils.logger_factory import log_exception 


class OkxExchangeAsync(ExchangeBase):
    def __init__(self, portfolio, logger):
        self.portfolio = portfolio
        self.client = None
        self.logger = logger

    async def __aenter__(self):
        self.client = await krex.okx(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            passphrase=self.portfolio["password"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_balance(self):
        return await self.client.get_account_balance()
    
    async def get_transfer_adjustment(self):
        params = {
                "type" : "1",          # 1 = Transfer
                "limit": str(100),
            } 
        return await self.client.get_account_bills(**params)

