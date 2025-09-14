# src/utils/mongo_serialization.py
from __future__ import annotations
from typing import Any, Dict
import numpy as np
import pandas as pd

__all__ = ["bsonify_row"]

def bsonify_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    將 pandas/numpy 值轉為 PyMongo 可接受型別；NaN/NaT/None 不寫入。
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