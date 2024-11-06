from dataclasses import dataclass

@dataclass
class DataPipelineConfig:
    geoencoding_url: str
    price_url: str
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str

@dataclass
class GeocodingResponse:
    id: str
    type_key: str  # NBH2 or NBH3
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
