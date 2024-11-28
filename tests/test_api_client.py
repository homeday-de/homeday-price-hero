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
    return APIClient(
        geoapi_key=settings.api.dev.geo_api_key, 
        priceapi_key=settings.api.dev.price_api_key
    )

@pytest.mark.asyncio
async def test_fetch_geocoding_data(api_client):
    base_url = settings.api.dev.geo_coding_url
    zipcode = "10315"
    name = "Ohne"
    
    # Mock the response data
    response_data_zip = geo_responses.get(zipcode)

    with aioresponses() as m:
        # Mock the expected URL and response
        m.get(f"{base_url}&postal_code={zipcode}", payload=response_data_zip)

        # Call the function
        result = await api_client.fetch_geocoding_data_by_zipcode(base_url, zipcode)
        
        # Verify the result
        assert isinstance(result, GeocodingResponse)
        assert result.geo_index == "10315"
        assert result.id == "NBH2DE75702"
        assert result.coordinates == {"lat": 52.50339854556861, "lng": 13.518376766536123}
        assert result.match_name == "Friedrichsfelde"

    response_data_city = geo_responses.get(name)

    with aioresponses() as m:
        # Mock the expected URL and response
        m.get(f"{base_url}&city={name}", payload=response_data_city)

        # Call the function
        result = await api_client.fetch_geocoding_data_by_name(base_url, name)
        
        # Verify the result
        assert isinstance(result, GeocodingResponse)
        assert result.geo_index == "Ohne"
        assert result.id == "AD08DE1992"
        assert result.coordinates == {"lat": 52.273521665147335, "lng": 7.27936823510655}
        assert result.match_name == "Ohne"

@pytest.mark.asyncio
async def test_fetch_price_data(api_client):
    base_url = settings.api.dev.price_url
    geoid = "NBH2DE75702"
    price_date = "2023-10-01"
    
    # Mock the response data
    response_data = price_responses.get(geoid)

    with aioresponses() as m:
        # Mock the expected URL and response
        m.get(f"{base_url}/{geoid}?price_date={price_date}", payload=response_data)

        # Call the function
        result = await api_client.fetch_price_data(base_url, geoid, price_date=price_date)
        
        # Verify the result
        assert isinstance(result, PriceResponse)
        assert result.place_id == geoid
        assert result.price_date == "2023-10-01"
        assert result.house_price.get("value") == 5027

@pytest.mark.asyncio
async def test_get_geo_data_in_batch(api_client):
    base_url = settings.api.dev.geo_coding_url
    zipcodes = ["10315", "12589"]
    cities = ["Ohne", "Ködnitz"]

    # Mock response data for each request in the batch
    response_data = geo_responses

    with aioresponses() as m:
        # Mock each URL and response
        m.get(f"{base_url}&postal_code=10315", payload=response_data.get(zipcodes[0]))
        m.get(f"{base_url}&postal_code=12589", payload=response_data.get(zipcodes[1]))

        # Call get_data_in_batch with fetch_geocoding_data as the fetch_function
        results = await api_client.get_data_in_batch(base_url, zipcodes, api_client.fetch_geocoding_data_by_zipcode)
        
        # Verify the results
        assert len(results) == 2
        assert isinstance(results, List)
        assert results[0].id == "NBH2DE75702"
        assert results[0].coordinates == {"lat": 52.50339854556861, "lng": 13.518376766536123}
        assert results[0].match_name == "Friedrichsfelde"
        assert results[1].id == "NBH2DE75693"
        assert results[1].coordinates == {"lat": 52.44183420284346, "lng": 13.705967663911409}
        assert results[1].match_name == "Rahnsdorf"

    with aioresponses() as m:
        # Mock each URL and response
        m.get(f"{base_url}&city=Ohne", payload=response_data.get(cities[0]))
        m.get(f"{base_url}&city=Ködnitz", payload=response_data.get(cities[1]))

        # Call get_data_in_batch with fetch_geocoding_data as the fetch_function
        results = await api_client.get_data_in_batch(base_url, cities, api_client.fetch_geocoding_data_by_name)
        
        # Verify the results
        assert len(results) == 2
        assert isinstance(results, List)
        assert results[0].id == "AD08DE1992"
        assert results[0].coordinates == {"lat": 52.273521665147335, "lng": 7.27936823510655}
        assert results[0].match_name == "Ohne"
        assert results[1].id == "AD08DE7589"
        assert results[1].coordinates == {"lat": 50.099291599932535, "lng": 11.510340529692305}
        assert results[1].match_name == "Ködnitz"

@pytest.mark.asyncio
async def test_get_price_data_in_batch(api_client):
    base_url = settings.api.dev.price_url
    idx_group = ["NBH2DE75702", "NBH2DE75693"]
    price_date = "2023-10-01"

    # Mock response data for each request in the batch
    response_data = price_responses

    with aioresponses() as m:
        # Mock each URL and response
        m.get(f"{base_url}/{idx_group[0]}?price_date={price_date}", payload=response_data.get(idx_group[0]))
        m.get(f"{base_url}/{idx_group[1]}?price_date={price_date}", payload=response_data.get(idx_group[1]))

        # Call get_data_in_batch with fetch_price_data as the fetch_function
        results = await api_client.get_data_in_batch(base_url, idx_group, api_client.fetch_price_data, price_date=price_date)
        
        # Verify the result
        assert len(results) == 2
        assert isinstance(results, List)
        assert results[0].place_id == 'NBH2DE75702'
        assert results[0].price_date == "2023-10-01"
        assert results[0].house_price.get("value") == 5027
        assert results[1].place_id == 'NBH2DE75693'
        assert results[1].price_date == "2023-10-01"
        assert results[1].apartment_price is None
