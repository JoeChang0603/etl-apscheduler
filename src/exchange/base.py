from abc import ABC, abstractmethod


class ExchangeBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_balance(self) -> dict:
        raise NotImplementedError

    # @abstractmethod
    # def get_positions(self) -> list:
    #     raise NotImplementedError

    # @abstractmethod
    # def get_current_price(self, symbols: list) -> dict:
    #     raise NotImplementedError
