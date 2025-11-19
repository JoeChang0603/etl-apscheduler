from datetime import datetime, timedelta

import krex.async_support as krex

from exchange.base import ExchangeBase
from utils.logger_factory import log_exception


class ZoomexExchangeAsync(ExchangeBase):
    """Context-managed Zoomex wrapper with simple balance retrieval."""

    def __init__(self, portfolio, logger):
        """Capture portfolio credentials and logging hooks.

        :param portfolio: Portfolio object containing Zoomex API keys.
        :param logger: Logger used for error reporting.
        """
        self.portfolio = portfolio
        self.client = None
        self.logger = logger

    async def __aenter__(self):
        """Create the krex Zoomex client and return ``self``."""
        self.client = await krex.zoomex(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Dispose the Zoomex client when leaving the context."""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    async def get_balance(self):
        """Return the wallet balance snapshot from Zoomex."""
        return await self.client.get_wallet_balance()
    
