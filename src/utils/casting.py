"""Strict casting helpers for parsing primitive values."""

from typing import Any

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def to_bool(value: Any) -> bool:
    """Parse booleans from strings while rejecting ambiguous values.

    :param value: Value to convert; accepts bools or truthy/falsy strings.
    :return: Parsed boolean value.
    :raises ValueError: If ``value`` cannot be interpreted as a boolean.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in _TRUE:  return True
        if s in _FALSE: return False
    raise ValueError(f"Cannot strictly parse bool from: {value!r}")
