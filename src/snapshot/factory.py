from snapshot.binance import BinanceSnapshotAsync
from snapshot.bingx import BingxSnapshotAsync
from snapshot.bitmex import BitmexSnapshotAsync
from snapshot.bybit import BybitSnapshotAsync
from snapshot.okx import OkxSnapshotAsync
from snapshot.zoomex import ZoomexSnapshotAsync


class SnapshotFactory:
    """Instantiate the correct snapshot handler based on portfolio exchange."""

    def __init__(self, portfolio, current_time, interval, logger):
        """Persist context used when constructing snapshot handlers.

        :param portfolio: Portfolio metadata including exchange identifier.
        :param current_time: Datetime used for the snapshot timestamp.
        :param interval: Interval in minutes used for adjustment lookups.
        :param logger: Logger injected into snapshot handlers.
        """
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    def get_handler(self):
        """Return the snapshot handler matching the portfolio exchange.

        :return: Snapshot handler instance ready for ``snapshot_account_summary``.
        :raises NotImplementedError: If the exchange is not supported.
        """
        if self.portfolio["exchange"] == "okx":
            return OkxSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        elif self.portfolio["exchange"] == "bybit":
            return BybitSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        elif self.portfolio["exchange"] == "binance":
            return BinanceSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        elif self.portfolio["exchange"] == "bingx":
            return BingxSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        elif self.portfolio["exchange"] == "bitmex":
            return BitmexSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        elif self.portfolio["exchange"] == "zoomex":
            return ZoomexSnapshotAsync(self.portfolio, self.current_time, self.interval, self.logger)
        else:
            raise NotImplementedError
