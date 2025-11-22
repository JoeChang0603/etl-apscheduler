from datetime import datetime
from typing import Any, Dict

import pytest


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:
        pass


@pytest.fixture
def dummy_logger() -> DummyLogger:
    return DummyLogger()


@pytest.fixture
def snapshot_time() -> datetime:
    return datetime(2025, 1, 1, 0, 0, 0)


@pytest.fixture
def portfolio_factory(exchange_keys):
    def _factory(exchange_name: str, **overrides: Dict[str, Any]) -> Dict[str, Any]:
        data = {
            "portfolio": f"{exchange_name}-portfolio",
            "exchange": exchange_name,
        }
        data.update(exchange_keys.get(exchange_name, {}))
        data.update(overrides)
        return data

    return _factory
