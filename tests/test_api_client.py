import pytest
import asyncio
from config import settings
from typing import List
from aioresponses import aioresponses
from src.api_client import APIClient
from src.models import GeocodingResponse, PriceResponse
from .mock_responses import geo_responses, price_responses


@pytest.fixture
def api_client():
    """Fixture to initialize the APIClient with mock API keys."""
    return APIClient(
        geoapi_key=settings.api.dev.geo_api_key, 
        priceapi_key=settings.api.dev.price_api_key
    )


@pytest.mark.asyncio
async def test_fetch_geocoding_data(api_client):
    """Test fetch_geocoding_data with both postal codes and city names."""
    base_url = settings.api.dev.geo_coding_url
    test_cases = [
        {
            "geo_obj": {"id": "no_hd_geo_id_applicable", "name": "10315"},
            "param": "postal_code",
            "response_data": geo_responses["10315"],
            "expected": {
                "geo_index": "10315",
                "id": "NBH2DE75702",
                "coordinates": {"lat": 52.50339854556861, "lng": 13.518376766536123},
                "match_name": "Friedrichsfelde"
            }
        },
        {
            "geo_obj": {"id": "3fdcc595-161c-57c0-b786-94bc424ea460", "name": "Ohne"},
            "param": "city",
            "response_data": geo_responses["Ohne"],
            "expected": {
                "geo_index": "Ohne",
                "id": "AD08DE1992",
                "coordinates": {"lat": 52.273521665147335, "lng": 7.27936823510655},
                "match_name": "Ohne"
            }
        }
    ]

    for case in test_cases:
        with aioresponses() as m:
            m.get(f"{base_url}&{case['param']}={case['geo_obj']['name']}", payload=case['response_data'])
            result = await api_client.fetch_geocoding_data(base_url, case["geo_obj"])
            assert isinstance(result, GeocodingResponse)
            assert result.geo_index == case["expected"]["geo_index"]
            assert result.id == case["expected"]["id"]
            assert result.coordinates == case["expected"]["coordinates"]
            assert result.match_name == case["expected"]["match_name"]


@pytest.mark.asyncio
async def test_fetch_price_data(api_client):
    """Test fetch_price_data for valid price responses."""
    base_url = settings.api.dev.price_url
    geoid = "NBH2DE75702"
    price_date = "2023-10-01"
    response_data = price_responses[geoid]

    with aioresponses() as m:
        m.get(f"{base_url}/{geoid}?price_date={price_date}", payload=response_data)
        result = await api_client.fetch_price_data(base_url, geoid, price_date=price_date)
        assert isinstance(result, PriceResponse)
        assert result.place_id == geoid
        assert result.price_date == price_date
        assert result.house_price.get("value") == 5027
