from datetime import datetime, timedelta

import krex.async_support as krex

from exchange.base import ExchangeBase
from utils.logger_factory import log_exception


class OkxExchangeAsync(ExchangeBase):
    """Context-managed OKX integration with balance utilities."""

    def __init__(self, portfolio, logger):
        """Persist OKX credentials and logger references.

        :param portfolio: Portfolio configuration containing OKX API data.
        :param logger: Logger used for diagnostic output.
        """
        self.portfolio = portfolio
        self.client = None
        self.logger = logger

    async def __aenter__(self):
        """Open the underlying krex OKX client and return ``self``."""
        self.client = await krex.okx(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            passphrase=self.portfolio["password"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close the OKX client after use."""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_balance(self):
        """Fetch the OKX account balance."""
        return await self.client.get_account_balance()
    
    async def get_transfer_adjustment(self):
        """Retrieve recent transfer adjustments from the account bills API."""
        params = {
                "type" : "1",          # 1 = Transfer
                "limit": str(100),
            } 
        return await self.client.get_account_bills(**params)
