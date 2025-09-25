"""Time and formatting utilities used across the project."""

from __future__ import annotations

import datetime
import time
from typing import Optional, Union

import ciso8601


def time_s() -> float:
    """Return the current wall-clock time in seconds as a float."""

    return time.time()


def time_ms() -> float:
    """Return the current wall-clock time in milliseconds as a float."""

    return time.time() * 1e3


def time_us() -> float:
    """Return the current wall-clock time in microseconds as a float."""

    return time.time() * 1e6


def time_ns() -> int:
    """Return the current wall-clock time in nanoseconds as an integer."""

    return time.time_ns()


def iso8601_to_unix(timestamp: str) -> float:
    """Convert an ISO-8601 string into a Unix timestamp.

    :param timestamp: ISO-8601 encoded datetime string.
    :return: Floating-point Unix timestamp in seconds.
    """

    return ciso8601.parse_datetime(timestamp).timestamp()


def unix_to_iso8601(timestamp: float) -> str:
    """Convert a Unix timestamp into an ISO-8601 string with precision.

    :param timestamp: Unix timestamp expressed in seconds, milliseconds,
        microseconds, or nanoseconds.
    :return: ISO-8601 formatted string with fractional seconds preserved.
    """

    if timestamp >= 1e18:  # nanoseconds
        seconds = timestamp / 1e9
        fractional_part = int(timestamp % 1e9)
        fractional_str = f"{fractional_part:09d}"
    elif timestamp >= 1e15:  # microseconds
        seconds = timestamp / 1e6
        fractional_part = int(timestamp % 1e6)
        fractional_str = f"{fractional_part:06d}"
    elif timestamp >= 1e12:  # milliseconds
        seconds = timestamp / 1e3
        fractional_part = int(timestamp % 1e3)
        fractional_str = f"{fractional_part:03d}"
    else:  # seconds
        seconds = timestamp
        fractional_part = int((timestamp % 1) * 1e9)
        fractional_str = f"{fractional_part:09d}"[:3]

    base_time = datetime.datetime.fromtimestamp(int(seconds)).isoformat(timespec="seconds")
    return f"{base_time}.{fractional_str}Z"


def time_iso8601() -> str:
    """Return the current UTC time formatted as ``YYYY-MM-DDTHH:MM:SS.fffZ``."""

    dt = datetime.datetime.now(datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def datetime_to_str(dt: datetime.datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Convert a datetime object into a formatted string.

    :param dt: Datetime instance to format.
    :param fmt: ``strftime``-compatible format string.
    :return: Formatted datetime string.
    """

    return dt.strftime(fmt)


def normalize_datetime(value: Optional[Union[str, datetime.datetime]]) -> Optional[datetime.datetime]:
    """Coerce an ISO-8601 string or datetime into a datetime object.

    :param value: Either ``None``, an ISO string, or a ``datetime`` instance.
    :return: Parsed datetime or ``None`` if the input was ``None``.
    :raises ValueError: If the string cannot be parsed into a datetime.
    :raises TypeError: If an unsupported type is supplied.
    """

    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            return ciso8601.parse_datetime(value)
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid datetime string: {value}") from exc
    raise TypeError(f"Unsupported datetime type: {type(value)!r}")
