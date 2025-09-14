from dataclasses import fields, is_dataclass


def model_parser(dataclass) -> dict:
    """
    Parse the data to get the model names and their attributes

    @dataclass
    class FundingRate:
        exchange: str
        product_type: str
        product_id: str
        funding_rate: Decimal
        funding_time: int
        next_funding_time: int
    ->
    {
        "exchange": FundingRate.exchange,
        "product_type": FundingRate.product_type,
        "product_id": FundingRate.product_id,
        "funding_rate": FundingRate.funding_rate,
        "funding_time": FundingRate.funding_time,
        ...
    }
    """
    if not is_dataclass(dataclass):
        raise TypeError("Input must be a dataclass")

    return {field.name: getattr(dataclass, field.name) for field in fields(dataclass)}
