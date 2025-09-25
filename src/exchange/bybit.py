from datetime import datetime, timedelta

import krex.async_support as krex

from exchange.base import ExchangeBase
from utils.logger_factory import log_exception


class BybitExchangeAsync(ExchangeBase):
    """Context-managed Bybit integration exposing balance utilities."""

    def __init__(self, portfolio, logger):
        """Assign portfolio credentials and logger handles.

        :param portfolio: Portfolio configuration containing API keys.
        :param logger: Logger used for exception reporting.
        """
        self.portfolio = portfolio
        self.client = None
        self.logger = logger
    
    async def __aenter__(self):
        """Open the krex Bybit client and return ``self``."""
        self.client = await krex.bybit(
            api_key=self.portfolio["api_key"],
            api_secret=self.portfolio["api_secret"],
            preload_product_table=False,
        )
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Close the Bybit client on context exit."""
        if self.client:
            await self.client.__aexit__(exc_type, exc, tb)

    async def get_balance(self):
        """Fetch wallet balance information from Bybit."""
        return await self.client.get_wallet_balance()
    
    async def get_transfer_adjustment(self, interval):
        """Calculate transfer adjustments within the provided interval.

        :param interval: Number of minutes to look back for transfer records.
        :return: Net adjustment amount based on internal transfers.
        """
        data = await self.client.get_internal_transfer_records()
        try:
            transactions = data["result"]["list"]
            adjustment = 0
            time_before = int((datetime.now() - timedelta(minutes=interval)).timestamp() * 1000)
            for transaction in transactions:
                if int(transaction["timestamp"]) > time_before:
                    if transaction["toAccountType"].lower() == "unified":
                        adjustment -= float(transaction["amount"])
                    else:
                        adjustment += float(transaction["amount"])
        except Exception as e:
            log_exception(self.logger, e, context="Bybit Exchange")
            return 0
        return adjustment
