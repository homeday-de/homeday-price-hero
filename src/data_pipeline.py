import asyncio
import configparser
from typing import List
from src.models import PriceResponse
from src.db import Database
from src.api_client import APIClient


class DataPipeline(Database):
    config = configparser.ConfigParser()
    config.read('config/config.dev.ini')
    
    def __init__(self, test=False):
        super().__init__(config=self.config, test=test)
        self.GEOCODING_URL = None
        self.PRICE_URL = None
        self.api = None

    def api_client(self, dev_env=True):
        api_conf = 'api.dev' if dev_env else 'api.preview'
        self.GEOCODING_URL = self.config.get(api_conf, 'geo_coding_url')
        self.PRICE_URL = self.config.get(api_conf, 'price_url')
        return APIClient(
            geoapi_key=self.config.get(api_conf, 'geo_api_key'),
            priceapi_key=self.config.get(api_conf, 'price_api_key')
        )

    async def run(self, zip_codes: List[str], price_date: str, dev_env=True):
        if not self.api:
            self.api = self.api_client(dev_env=dev_env)
        self.create_database()
        self.create_tables()
        prices_responses = await self.fetch_price(zip_codes, price_date)
        for prices_response in prices_responses:
            self.store_price_in_db(prices_response)
            
        self.close_db_connection()

    async def fetch_price(self, zip_codes: List[str], price_date: str) -> PriceResponse:
        cached_geoid = await self.ensure_geoid_cache(zip_codes)
        return await self.api.get_data_in_batch(
            self.PRICE_URL, cached_geoid, self.api.fetch_price_data, price_date=price_date
        )
    
    async def ensure_geoid_cache(self, zip_codes: List[str]) -> List[str]:
        """Retrieve or fetch and cache geo_id for a given list of zip codes."""
        cached_geoid = self.get_cached_geoid(zip_codes)
        # Fetch and cache if not found
        if not cached_geoid or len(cached_geoid) != len(zip_codes):
            await self.fetch_geo(zip_codes)
            cached_geoid = self.get_cached_geoid(zip_codes)
        
        return cached_geoid

    async def fetch_geo(self, zip_codes: List[str]):
        geocoding_responses = await self.api.get_data_in_batch(self.GEOCODING_URL, zip_codes, self.api.fetch_geocoding_data)
        for geocoding_response in geocoding_responses:
            self.cache_geo_response(geocoding_response)

    def close_db_connection(self):
        if self.conn:
            self.conn.close()
