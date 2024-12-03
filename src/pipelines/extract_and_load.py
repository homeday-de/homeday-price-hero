import asyncio
import datetime
import json
import logging
from typing import List, Dict
from dynaconf import Dynaconf
from src.models import PriceResponse
from src.db import Database
from src.api_client import APIClient
from src.lib.aws import S3Connector


class APIToPostgres(Database):
    
    def __init__(self, config: Dynaconf, test=False):
        super().__init__(config=config, test=test)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.GEOCODING_URL = None
        self.PRICE_URL = None
        self.api = None
        self.api_config = config.api.dev if test else config.api.preview

    def api_client(self):
        self.GEOCODING_URL = self.api_config.geo_coding_url
        self.PRICE_URL = self.api_config.price_url
        return APIClient(
            geoapi_key=self.api_config.geo_api_key,
            priceapi_key=self.api_config.price_api_key
        )

    async def run(self, geo_indices: Dict, price_date: str):
        if not self.api:
            self.api = self.api_client()
        try:
            self.logger.info("Starting extraction pipeline...")
            self.create_database()
            self.create_tables()
            prices_responses = await self.fetch_price(geo_indices, price_date)
            for prices_response in prices_responses:
                self.store_price_in_db(prices_response)
            self.logger.info("Prices info has been cached")
        finally:
            self.close_db_connection()
            self.logger.info("Pipeline execution completed.")

    async def fetch_price(self, geo_indices: Dict, price_date: str) -> PriceResponse:
        cached_geoid = await self.ensure_geoid_cache(geo_indices)
        return await self.api.get_data_in_batch(
            self.PRICE_URL, cached_geoid, self.api.fetch_price_data, price_date=price_date
        )
    
    async def ensure_geoid_cache(self, geo_indices: Dict) -> List[str]:
        """Retrieve or fetch and cache geo_id for a given list of zip codes."""
        zipcode_in_list = [geo_indices['zip_codes'][i]['name'] for i, _ in enumerate(geo_indices['zip_codes'])]
        cityname_in_list = [geo_indices['cities'][i]['name'] for i, _ in enumerate(geo_indices['cities'])]
        all_index = zipcode_in_list + cityname_in_list
        cached_geoid = self.get_cached_geoid(all_index)
        # Fetch and cache if not found
        if not cached_geoid or len(cached_geoid) != len(all_index):
            await self.fetch_geo(geo_indices)
            cached_geoid = self.get_cached_geoid(all_index)
        self.logger.info('Geo info has been cached')
        return cached_geoid

    async def fetch_geo(self, geo_indices: Dict):
        geocoding_responses_zip = await self.api.get_data_in_batch(self.GEOCODING_URL, geo_indices['zip_codes'], self.api.fetch_geocoding_data)
        for geocoding_response in geocoding_responses_zip:
            self.cache_geo_response(geocoding_response)
        geocoding_responses_city = await self.api.get_data_in_batch(self.GEOCODING_URL, geo_indices['cities'], self.api.fetch_geocoding_data)
        for geocoding_response in geocoding_responses_city:
            self.cache_geo_response(geocoding_response)


class PostgresToS3(Database):
    def __init__(self, config: Dynaconf, s3_connector: S3Connector, test: bool):
        """
        Initialize the PostgresToS3 class.
        :param config: Database connection parameters.
        :param s3_connector: An instance of S3Connector for S3 operations.
        """
        super().__init__(config=config, test=test)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3_connector = s3_connector

    def run(self, table_name, transformed=False, local=True):
        # Example configuration and usage
        quarter = datetime.date.today().strftime("%Y%m")  # e.g., "2024Q3"
        subfolder = "source_dump" if not transformed else "transformed"
        s3_key = f"{subfolder}/{table_name}_{quarter}.json"  # Desired S3 key

        if not local:
            # Dump table and upload to S3
            self.dump_and_upload(table_name, s3_key)
        else:
            data = self.dump_table_to_json(table_name)
            self.save_json_to_file(data, file_path=f"data/{table_name}.json")

    def dump_table_to_json(self, table_name: str) -> List[Dict]:
        """
        Dump the contents of a PostgreSQL table into a JSON object.
        :param table_name: The name of the table to dump.
        :return: A list of dictionaries representing the table rows.
        """
        if not self.conn:
            self.connect_to_db()
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            self.logger.error(f"Error dumping table {table_name} to JSON: {e}")
            return []

    def dump_and_upload(self, table_name: str, s3_key: str):
        """
        Dump a PostgreSQL table to JSON and upload it to S3.
        :param table_name: The name of the PostgreSQL table.
        :param s3_key: The S3 key (object name) for the uploaded JSON file.
        """
        self.logger.info(f"Dumping table '{table_name}' to JSON...")
        table_data = self.dump_table_to_json(table_name)
        if table_data:
            self.logger.info(f"Uploading table data to S3 bucket: {self.s3_connector.bucket_name}, key: {s3_key}")
            self.s3_connector.upload_json_data(table_data, s3_key)
        else:
            self.logger.info(f"No data to upload for table {table_name}.")

    def save_json_to_file(self, data: List[Dict], file_path: str):
        """
        Save JSON data to a local file.
        :param data: The JSON data to save (list of dictionaries).
        :param file_path: The path of the file to save the JSON data to.
        """
        try:
            with open(file_path, "w") as json_file:
                json.dump(data, json_file, indent=4)
            self.logger.info(f"Data successfully saved to {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving JSON to file {file_path}: {e}")
