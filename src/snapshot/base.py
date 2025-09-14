from abc import ABC, abstractmethod


class SnapshotBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def snapshot_account_summary(self) -> dict:
        raise NotImplementedError