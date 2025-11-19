from datetime import datetime, timedelta

import krex.async_support as krex

from exchange.base import ExchangeBase
from utils.logger_factory import log_exception


class BinanceExchangeAsync(ExchangeBase):
    """Context-manager wrapper around the asynchronous Binance client."""

    def __init__(self, portfolio, logger):
        """Store portfolio credentials and target logger.

        :param portfolio: Portfolio document containing API credentials.
        :param logger: Logger used for error reporting.
        """
        self.portfolio = portfolio
        self.client = None
        self.logger = logger
    
    async def __aenter__(self):
        """Open the underlying krex Binance client and return ``self``."""
        self.client = await krex.binance(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close the krex Binance client when leaving the context."""
        if self.client:
            await self.client.__aexit__(exc_type, exc, tb)

    async def get_balance(self, market_type: str = "spot"):
        """Return account balances for the requested market type.

        :param market_type: Binance market type (e.g. ``spot``).
        :return: Raw response payload from the Binance API.
        """
        return await self.client.get_account_balance(market_type = market_type)
    
    async def get_spot_price(self):
        """Fetch spot price table for all supported symbols."""
        return await self.client.get_spot_price()
    
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
