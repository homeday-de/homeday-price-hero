import asyncio
import datetime
import json
import logging
from collections import deque
from typing import List, Dict, Union, Callable
from dynaconf import Dynaconf
from src.db import Database
from src.api_client import APIClient
from src.lib.aws import S3Connector
from src.lib import benchmark


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
    
    async def process_data_in_batch(
            self,
            base_url: str,
            idx_group: Union[List[str], List[Dict]],
            fetch_function: Callable,
            cache_function: Callable,
            batch_size: int,
            rate_limit_interval: float,
            **kwargs
        ):
        """
        Process data in batches with optimized handling of batch fetches and caching.

        :param base_url: Base URL for the fetch function.
        :param idx_group: List of indices or objects to process.
        :param fetch_function: Function to fetch data.
        :param cache_function: Function to store or cache results.
        :param batch_size: Size of each batch.
        :param rate_limit_interval: Delay between processing batches.
        :param kwargs: Additional arguments for fetch_function.
        """
        # Pre-split batches and prepare queues
        batches = [idx_group[i:i + batch_size] for i in range(0, len(idx_group), batch_size)]
        total_batches = len(batches)
        self.logger.info(f"Starting data fetching from {fetch_function.__name__} in {total_batches} batches...")

        async def process_single_batch(batch, batch_index):
            """
            Process a single batch: fetch data and cache results.
            """
            self.logger.info(f"Processing batch {batch_index}/{total_batches}...")
            results = await asyncio.gather(*(fetch_function(base_url, unit, **kwargs) for unit in batch))
            for result in results:
                if result:
                    cache_function(result)
            self.logger.info(f"Batch {batch_index} is cached")

        # Process batches with rate limiting
        for index, batch in enumerate(batches, start=1):
            await process_single_batch(batch, index)
            if index < total_batches:  # Skip delay after the last batch
                await asyncio.sleep(rate_limit_interval)

    @benchmark(enabled=True)
    async def run(self, geo_indices: Dict, price_date: str):
        if not self.api:
            self.api = self.api_client()
        try:
            self.logger.info("Starting extraction pipeline...")
            self.create_database()
            self.create_tables()
            await self.ensure_geoid_cache(geo_indices)
            await self.fetch_price(price_date)
            self.logger.info("Prices info has been cached")
        finally:
            self.close_db_connection()
            self.logger.info("Pipeline execution completed.")

    async def fetch_price(self, price_date: str):
        cached_geoid = self.get_validated_price(price_date)
        await self.process_data_in_batch(
            self.PRICE_URL, cached_geoid, self.api.fetch_price_data, self.store_price_in_db,
            self.api.batch_size, self.api.rate_limit_interval, price_date=price_date
        )
    
    async def ensure_geoid_cache(self, geo_indices: Dict):
        """Retrieve or fetch and cache geo_id for a given list of zip codes."""
        _all = geo_indices['zip_codes'] + geo_indices['cities']
        not_cached_yet = []
        for obj in _all:
            cached_geoid = self.get_cached_geoid([obj['name']])
            if not cached_geoid:
                not_cached_yet.append(obj)
        self.logger.info(f"Found {len(not_cached_yet)} geo_indices haven't been cached yet")
        await self.fetch_geo(not_cached_yet)

    async def fetch_geo(self, index_group: List[Dict]):
        await self.process_data_in_batch(
            self.GEOCODING_URL, index_group, self.api.fetch_geocoding_data, self.cache_geo_response, 
            self.api.batch_size, self.api.rate_limit_interval)


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
