import logging
import aiohttp
from typing import Dict
from aiohttp.client_exceptions import ClientConnectorDNSError, ContentTypeError
from src.models import GeocodingResponse, PriceResponse


class APIClient:
    
    batch_size = 50
    rate_limit = 75  # requests per second
    rate_limit_interval = (batch_size / rate_limit) * 2  # seconds between requests

    def __init__(self, geoapi_key, priceapi_key):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.geo_api_key = geoapi_key
        self.price_api_key = priceapi_key
            
    async def fetch_geocoding_data(self, base_url: str, geo_obj: Dict) -> GeocodingResponse:
        headers = {'X-Api-Key': f"{self.geo_api_key}"}
        async with aiohttp.ClientSession() as session:
            param = 'postal_code' if geo_obj['id'] == 'no_hd_geo_id_applicable' else 'city'
            url = f"{base_url}&{param}={geo_obj['name']}"
            try:
                async with session.get(url, headers=headers) as response:
                    res = await response.json()
                    # Select the first match from the responses because it has the lowest geographic granularity
                    data = res.get('items', {}).get('aviv', {})[0].get('match', {})
                    return GeocodingResponse(geo_obj['name'], geo_obj['id'], **data)
            except aiohttp.client_exceptions.ClientConnectorDNSError as e:
                self.logger.error(f"Please check the connection well-connected via Cloudflare. Error: {e}")

    async def fetch_price_data(self, base_url: str, geoid: str, price_date: str) -> PriceResponse:
        headers = {'X-Api-Key': f"{self.price_api_key}"}
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}/{geoid}?price_date={price_date}"
            no_entity_placeholder = {
                "aviv_geo_id": geoid, "price_date": price_date, "transaction_type": "TRANSACTION_TYPE.SELL",
                "house_price": {}, "apartment_price": {}, "hybrid_price": {}
            }
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 404:
                        return PriceResponse(**no_entity_placeholder)
                    res = await response.json()
                    data = res.get('items', {})[0]
                    return PriceResponse(**data)
            except aiohttp.client_exceptions.ClientConnectorDNSError as e:
                self.logger.error(f"Please check the connection well-connected via Cloudflare. Error: {e}")
