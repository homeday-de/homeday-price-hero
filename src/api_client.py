import aiohttp
import asyncio
from collections import deque
from typing import List, Callable
from src.models import GeocodingResponse, PriceResponse


class APIClient:
    
    batch_size = 10
    rate_limit = 50  # requests per minute
    rate_limit_interval = 60 / rate_limit  # seconds between requests

    def __init__(self, geoapi_key, priceapi_key):
        self.geo_api_key = geoapi_key
        self.price_api_key = priceapi_key

    async def get_data_in_batch(self, base_url: str, idx_group: List[str], fetch_function: Callable, **kwargs) -> List:
    # Ensure fetch_function is a method of the current instance
        if not callable(fetch_function) or not hasattr(self, fetch_function.__name__):
            raise ValueError("fetch_function must be a method of the current instance")

        results = []

        batches = [idx_group[i:i + self.batch_size] for i in range(0, len(idx_group), self.batch_size)]
        batch_queue = deque(batches)

        while batch_queue:
            batch = batch_queue.popleft()
            coros = [fetch_function(base_url, unit, **kwargs) for unit in batch]
            batch_results = await asyncio.gather(*coros)
            results.extend(batch_results)
            await asyncio.sleep(self.rate_limit_interval)

        return results

    async def fetch_geocoding_data_by_zipcode(self, base_url: str, zipcode: str) -> GeocodingResponse:
        headers = {'X-Api-Key': f"{self.geo_api_key}"}
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}&postal_code={zipcode}"
            async with session.get(url, headers=headers) as response:
                res = await response.json()
                # Select the first match from the responses because it has the lowest geographic granularity
                data = res.get('items', {}).get('aviv', {})[0].get('match', {})
                return GeocodingResponse(zipcode, **data)
    
    async def fetch_geocoding_data_by_name(self, base_url: str, name: str) -> GeocodingResponse:
        headers = {'X-Api-Key': f"{self.geo_api_key}"}
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}&city={name}"
            async with session.get(url, headers=headers) as response:
                res = await response.json()
                # Select the first match from the responses because it has the lowest geographic granularity
                data = res.get('items', {}).get('aviv', {})[0].get('match', {})
                return GeocodingResponse(name, **data)

    async def fetch_price_data(self, base_url: str, geoid: str, price_date: str) -> PriceResponse:
        headers = {'X-Api-Key': f"{self.price_api_key}"}
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}/{geoid}?price_date={price_date}"
            async with session.get(url, headers=headers) as response:
                if response.status == 404:
                    return
                res = await response.json()
                data = res.get('items', {})[0]
                return PriceResponse(**data)
