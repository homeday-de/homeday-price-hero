from dataclasses import dataclass


@dataclass
class GeocodingResponse:
    geo_index: str # postal_code or city
    hd_geo_id: str # homeday geo id
    id: str
    type_key: str  # NBH2 or AD08
    coordinates: dict
    bounding_box: dict
    match_name: str
    confidence_score: int
    parents: list

@dataclass
class PriceResponse:
    place_id: str
    price_date: str
    transaction_type: str
    house_price: dict
    apartment_price: dict
    hybrid_price: dict
