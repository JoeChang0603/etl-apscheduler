from datetime import datetime, timedelta

import krex.async_support as krex

from exchange.base import ExchangeBase
from utils.logger_factory import log_exception


class BitmartExchangeAsync(ExchangeBase):
    """Context-managed wrapper for Bitmart asynchronous client operations."""

    def __init__(self, portfolio, logger):
        """Persist portfolio credentials and logger references.

        :param portfolio: Portfolio configuration containing API credentials.
        :param logger: Logger used for diagnostic messages.
        """
        self.portfolio = portfolio
        self.client = None
        self.logger = logger
    
    async def __aenter__(self):
        """Initialise the Bitmart client and return ``self``."""
        self.client = await krex.bitmart(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            memo=self.portfolio["memo"]
        )
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close the Bitmart client on context exit."""
        if self.client:
            await self.client.__aexit__(exc_type, exc, tb)

    async def get_balance(self):
        """Retrieve the unified account balance from BingX."""
        return await self.client.get_account_balance()
    
    async def get_trading_pairs_details(self):
        """Fetch contract specifications for supported trading pairs."""
        return await self.client.get_trading_pairs_details()
    
    async def get_ticker_of_a_pair(self, product_symbol: str):
        """Return the latest ticker information for ``product_symbol``."""
        return await self.client.get_ticker_of_a_pair(product_symbol)
