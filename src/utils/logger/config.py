"""Configuration objects and enums used by the logging subsystem."""

from dataclasses import dataclass
from enum import IntEnum


class LogLevel(IntEnum):
    """Severity levels understood by the custom logger implementation."""

    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


@dataclass
class LogEvent:
    """A single log message captured for buffering/dispatch."""

    text: str
    level: LogLevel


class LoggerConfig:
    """Runtime configuration for :class:`utils.logger.logger.Logger`."""

    def __init__(
        self,
        base_level: LogLevel = LogLevel.INFO,
        do_stdout: bool = True,
        str_format: str = "%(asctime)s %(icon)s [%(levelname)s] %(name)s - %(message)s",
        buffer_capacity: int = 100,
        buffer_timeout: float = 5.0,
    ):
        """Initialise configuration defaults for a :class:`Logger`.

        :param base_level: Minimum severity that will be recorded.
        :param do_stdout: Whether messages are mirrored to stdout.
        :param str_format: Format string applied to log messages.
        :param buffer_capacity: Maximum buffered events before a flush.
        :param buffer_timeout: Maximum seconds before the buffer auto-flushes.
        :raises ValueError: If validation of supplied values fails.
        """
        self.base_level = base_level
        self.do_stdout = do_stdout

        self.buffer_capacity = buffer_capacity
        self.buffer_timeout = buffer_timeout

        if not isinstance(self.buffer_capacity, int):
            raise ValueError(f"Invalid buffer capacity; expected int but got {type(self.buffer_capacity)}")

        if self.buffer_capacity < 1:
            raise ValueError(f"Invalid buffer capacity; expected >1 but got {self.buffer_capacity}")

        if self.buffer_timeout <= 0.0:
            raise ValueError(f"Invalid buffer timeout; expected >0 but got {self.buffer_timeout}")

        self.str_format = str_format

        if "%(message)s" not in self.str_format:
            raise ValueError("Format string must contain '%(message)s' placeholder")
