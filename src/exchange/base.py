from abc import ABC, abstractmethod


class ExchangeBase(ABC):
    """Base class defining the minimal surface for exchange clients."""

    def __init__(self) -> None:
        """Initialise the exchange base class."""
        pass

    @abstractmethod
    def get_balance(self) -> dict:
        """Return the current account balance for the integration."""
        raise NotImplementedError

    # @abstractmethod
    # def get_positions(self) -> list:
    #     raise NotImplementedError

    # @abstractmethod
    # def get_current_price(self, symbols: list) -> dict:
    #     raise NotImplementedError
