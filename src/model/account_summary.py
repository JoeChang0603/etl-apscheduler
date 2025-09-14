from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime


@dataclass
class AssetBalance:
    total: Decimal
    available: Decimal
    notional: Decimal
    liability: Decimal
    interest: Decimal


@dataclass
class AccountSummary:
    portfolio: str
    exchange: str
    total_usd_value: Decimal
    balances: dict  # {
    #     "USDT": {
    #         "total": Decimal,
    #         "available": Decimal,
    #         "notional": Decimal,
    #         "liability": Decimal,
    #         "interest": Decimal,
    #      },
    #     "BTC": {
    #         "total": Decimal,
    #         "available": Decimal,
    #         "notional": Decimal,
    #         "liability": Decimal,
    #         "interest": Decimal,
    #     }...
    # }
    transfer_adjustment: Decimal
    current_time: datetime
    tw_time: datetime
