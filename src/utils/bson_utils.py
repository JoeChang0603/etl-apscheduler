# src/utils/mongo_serialization.py
"""Helpers for converting pandas/numpy rows into BSON-safe dictionaries."""

from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd

__all__ = ["bsonify_row"]


def bsonify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert pandas/numpy values into BSON-friendly primitives.

    :param row: Mapping of column names to scalar values produced by pandas.
    :return: Dictionary containing only BSON-compatible primitives.
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if pd.isna(v):
            continue
        if isinstance(v, pd.Timestamp):
            v = v.to_pydatetime()
        elif isinstance(v, (np.integer,)):  v = int(v)
        elif isinstance(v, (np.floating,)): v = float(v)
        elif isinstance(v, (np.bool_,)):    v = bool(v)
        out[k] = v
    return out
