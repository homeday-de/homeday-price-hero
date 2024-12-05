import logging
import aiohttp
from typing import Dict, List
from aiohttp.client_exceptions import ClientConnectorDNSError, ContentTypeError
from src.models import GeocodingResponse, PriceResponse


class APIClient:
    batch_size = 50
    rate_limit = 100  # requests per second
    rate_limit_interval = (batch_size / rate_limit) * 2  # seconds between requests

    def __init__(self, geoapi_key: str, priceapi_key: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.geo_api_key = geoapi_key
        self.price_api_key = priceapi_key

    async def _make_request(self, url: str, headers: Dict[str, str]) -> Dict:
        """Helper method to make HTTP GET requests."""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status in {200, 404}:
                        return await response.json()
                    else:
                        self.logger.error(f"Unexpected status {response.status} for URL: {url}")
                        return {}
            except (ClientConnectorDNSError, ContentTypeError) as e:
                self.logger.error(f"Connection error for URL: {url}. Error: {e}")
                return {}

    async def fetch_geocoding_data(self, base_url: str, geo_obj: Dict) -> GeocodingResponse:
        headers = {'X-Api-Key': self.geo_api_key}
        param_key = 'postal_code' if geo_obj['id'] == 'no_hd_geo_id_applicable' else 'city'
        url = f"{base_url}&{param_key}={geo_obj['name']}"

        response = await self._make_request(url, headers)
        if response:
            data = self._validate_geocoding_data(response.get('items', {}).get('aviv', []))
            if data:
                return GeocodingResponse(geo_obj['name'], geo_obj['id'], **data)

        self.logger.error(f"Failed to fetch geocoding data from aviv for geo_obj: {geo_obj}")
        return self._default_geocoding_response(geo_obj['name'], geo_obj['id'])

    async def fetch_price_data(self, base_url: str, geoid: str, price_date: str) -> PriceResponse:
        headers = {'X-Api-Key': self.price_api_key}
        url = f"{base_url}/{geoid}?price_date={price_date}"

        response = await self._make_request(url, headers)
        if response:
            if response.get('items'):
                data = response['items'][0]
                return PriceResponse(**data)

        self.logger.error(f"Failed to fetch price data for geoid: {geoid}, price_date: {price_date}")
        return self._default_price_response(geoid, price_date)
    
    @staticmethod
    def _default_price_response(geoid: str, price_date: str) -> PriceResponse:
        """Generate a default PriceResponse when no entity is found."""
        return PriceResponse(
            place_id=geoid,
            price_date=price_date,
            transaction_type=None,
            house_price={},
            apartment_price={},
            hybrid_price={}
        )
    
    @staticmethod
    def _default_geocoding_response(geo_index: str, hd_geo_id: str) -> GeocodingResponse:
        """Generate a default GeocodingResponse when no entity is found."""
        return GeocodingResponse(
            geo_index=geo_index,
            hd_geo_id=hd_geo_id,
            id=None,
            type_key=None,
            coordinates={},
            bounding_box={},
            match_name=None,
            confidence_score=0,
            parents=None
        )

    def _validate_geocoding_data(self, items: List[Dict]) -> Dict:
        """
        Validate geocoding data to select the appropriate unit.

        :param items: List of geocoding response items.
        :return: Validated geocoding data dictionary or empty dictionary if none found.
        """
        if not items:
            return {}

        # Default to the first unit
        selected_unit = items[0]
        
        # Check for 'type_key' and adjust selection if necessary
        if selected_unit.get('match').get('type_key') == 'POCO' and len(items) > 1:
            selected_unit = items[1]

        return selected_unit.get('match', {})
