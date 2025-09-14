from snapshot.bybit import BybitSnapshotAsync
from snapshot.okx import OkxSnapshotAsync
from snapshot.binance import BinanceSnapshotAsync
from snapshot.bingx import BingxSnapshotAsync
from snapshot.bitmex import BitmexSnapshotAsync
from snapshot.zoomex import ZoomexSnapshotAsync

class SnapshotFactory:
    def __init__(self, portfolio, current_time, interval, logger):
        self.portfolio = portfolio
        self.current_time = current_time
        self.interval = interval
        self.logger = logger

    def get_handler(self):
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
