from typing import Any

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

def to_bool(value: Any) -> bool:
    """
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in _TRUE:  return True
        if s in _FALSE: return False
    raise ValueError(f"Cannot strictly parse bool from: {value!r}")
