from abc import ABC, abstractmethod


class SnapshotBase(ABC):
    """Base type for snapshot implementations."""

    def __init__(self) -> None:
        """Initialise the snapshot base class."""
        pass

    @abstractmethod
    def snapshot_account_summary(self) -> dict:
        """Return an account summary structure for the portfolio."""
        raise NotImplementedError
