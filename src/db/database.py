import psycopg
import json
from typing import List
from psycopg import sql
from dynaconf import Dynaconf
from src.models import GeocodingResponse, PriceResponse
from src.db.schema import create_, insert_


class Database:
    def __init__(self, config: Dynaconf, test=False):
        self.conn = None
        self.db_config = config.db.dev if not test else config.db.test
        
    def create_database(self):
        try:
            # Connect to the PostgreSQL server
            with psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                dbname="postgres",
                user=self.db_config.user,
                password=self.db_config.password
            ) as conn:
                conn.autocommit = True  # Enable autocommit for DDL commands
                with conn.cursor() as cur:
                    # Check if the database exists
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_config.name,))
                    exists = cur.fetchone()
                    
                    # Create the database if it does not exist
                    if not exists:
                        cur.execute(f"CREATE DATABASE {self.db_config.name};")
                        print(f"Database '{self.db_config.name}' created successfully.")
                    else:
                        print(f"Database '{self.db_config.name}' already exists.")
        except Exception as error:
            print("Error creating database:", error)

    def connect_to_db(self):
        try:
            self.conn = psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                dbname=self.db_config.name,
                user=self.db_config.user,
                password=self.db_config.password
            )
        except (Exception, psycopg.Error) as error:
            print("Error connecting to PostgreSQL:", error)
            if f'FATAL:  database "{self.db_config.name}" does not exist' in str(error):
                self.create_database()

    def create_tables(self):
        """Create required tables in the database if they do not exist."""
        if not self.conn:
            self.connect_to_db()
        with self.conn.cursor() as cur:
            cur.execute(create_['prices_all'])
            cur.execute(create_['geo_cache'])
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            for table in create_['prices_mapped']:
                cur.execute(table)
            # To avoid manual work in prices db, reset the id starting from 30 to avoid index conflict
            cur.execute('ALTER SEQUENCE report_batches_id_seq RESTART WITH 30;')
            self.conn.commit()

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
                geocoding_response.id,
                geocoding_response.type_key,
                json.dumps(geocoding_response.coordinates),
                geocoding_response.match_name,
                geocoding_response.confidence_score
            )
            cur.execute(insert_['geo_cache'], (geocoding_response.geo_index,)+geocoding_data)
            self.conn.commit()

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
