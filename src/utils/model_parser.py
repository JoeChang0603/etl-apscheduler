"""Helpers for flattening dataclass instances into dictionaries."""

from dataclasses import fields, is_dataclass
from typing import Any, Dict


def model_parser(dataclass_obj: Any) -> Dict[str, Any]:
    """Convert a dataclass instance into a plain dictionary.

    :param dataclass_obj: Dataclass instance to serialise.
    :return: Dictionary mapping field names to their values.
    :raises TypeError: If ``dataclass_obj`` is not a dataclass instance.
    """

    if not is_dataclass(dataclass_obj):
        raise TypeError("Input must be a dataclass")

    return {field.name: getattr(dataclass_obj, field.name) for field in fields(dataclass_obj)}
