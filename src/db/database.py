import psycopg
import json
import logging
from typing import List, Dict
from psycopg import sql
from dynaconf import Dynaconf
from src.models import GeocodingResponse, PriceResponse
from src.db.schema import create_, insert_, create_price_map_schema


class Database:
    def __init__(self, config: Dynaconf, test=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn = None
        self.db_config = config.db.dev if not test else config.db.test
        self.db_params = config.db.params
        
    def create_database(self):
        try:
            # Connect to the PostgreSQL server
            with psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                dbname="postgres",
                user=self.db_config.username,
                password=self.db_config.password
            ) as conn:
                conn.autocommit = True  # Enable autocommit for DDL commands
                with conn.cursor() as cur:
                    # Check if the database exists
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_config.database,))
                    exists = cur.fetchone()
                    
                    # Create the database if it does not exist
                    if not exists:
                        cur.execute(f"CREATE DATABASE {self.db_config.database};")
                        self.logger.info(f"Database '{self.db_config.database}' created successfully.")
                    else:
                        self.logger.info(f"Database '{self.db_config.database}' already exists.")
        except Exception as error:
            self.logger.error("Error creating database:", error)

    def connect_to_db(self):
        try:
            self.conn = psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                dbname=self.db_config.database,
                user=self.db_config.username,
                password=self.db_config.password
            )
        except (Exception, psycopg.Error) as error:
            self.logger.error("Error connecting to PostgreSQL:", error)
            if f'FATAL:  database "{self.db_config.name}" does not exist' in str(error):
                self.create_database()

    def create_tables(self):
        """Create required tables in the database if they do not exist."""
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            # Create landing tables for API response
            self.execute_nested_query_structure(cur, create_)
            
            # Create a mapping to the HD prices database
            report_batches = create_price_map_schema['report_batches']
            self.execute_nested_query_structure(cur, report_batches)

            report_headers = create_price_map_schema['report_headers']
            self.execute_nested_query_structure(cur, report_headers)

            location_prices = create_price_map_schema['location_prices']
            self.execute_nested_query_structure(cur, location_prices)
            
            # Get the latest updated report_batches_id_seq numbers from the config
            cur.execute(
                sql.SQL("ALTER SEQUENCE {} RESTART WITH {}").format(
                    sql.Identifier('report_batches_id_seq'),
                    sql.Literal(self.db_params.report_batch_id)
                )
            )
            self.conn.commit()

    def execute_nested_query_structure(self, cursor, nest: Dict):
        for name, query in nest.items():
            self.logger.info(f"Creating {name}...")
            cursor.execute(query)

    def get_cached_geoid(self, geo_index: List[str]):
        """Retrieve cached geo_id for a given zip code."""
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            select_query = sql.SQL(
                """
                SELECT aviv_geo_id FROM geo_cache 
                WHERE geo_index IN (
                {}
                )
                """
            ).format(sql.SQL(', ').join(sql.Placeholder() for _ in geo_index))
            cur.execute(select_query, geo_index)
            results = cur.fetchall()
        return [result[0] for result in results] if results else None

    def cache_geo_response(self, geocoding_response: GeocodingResponse):
        """Cache geocoding response data in the geo_cache table."""
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            geocoding_data = (
                geocoding_response.geo_index,
                geocoding_response.hd_geo_id,
                geocoding_response.id,
                geocoding_response.type_key,
                json.dumps(geocoding_response.coordinates),
                geocoding_response.match_name,
                geocoding_response.confidence_score
            )
            cur.execute(insert_['geo_cache'], geocoding_data)
            self.conn.commit()

    def get_validated_price(self, price_date: str):
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            select_query = sql.SQL(
                    f"""
                    SELECT aviv_geo_id FROM geo_cache
                    WHERE aviv_geo_id != 'no_aviv_id_available'
                    AND aviv_geo_id NOT IN (
                        SELECT aviv_geo_id FROM prices_all 
                        WHERE price_date = '{price_date}' AND 
                        transaction_type IS NOT NULL
                        )
                    """
                )
            cur.execute(select_query)
            results = cur.fetchall()
        return [result[0] for result in results] if results else None

    def store_price_in_db(self, price_response: PriceResponse):
        """Store price response data in the prices_all table."""
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            if price_response:
                price_data = (
                    price_response.place_id,
                    price_response.price_date,
                    price_response.transaction_type,
                    json.dumps(price_response.house_price),
                    json.dumps(price_response.apartment_price),
                    json.dumps(price_response.hybrid_price)
                )
                cur.execute(insert_['prices_all'], price_data)
                self.conn.commit()

    def close_db_connection(self):
        if self.conn:
            self.conn.close()
