import psycopg
import json
import logging
from typing import List, Dict, Union
from psycopg import sql
from dynaconf import Dynaconf
from src.models import GeocodingResponse, PriceResponse
from src.db.query_base import CREATE_DB, CHECK_DB_EXISTENCE, RESET_SEQUENCE
from src.db.query_base import create_source_schema, create_price_map_schema, insert_source
from src.db.query_base import REFLECT_AVIVID, VALIDATE_PRICE_GEN, GET_SEQUENCE_VALUE


class DatabaseHandler:
    """A reusable database handler for establishing and managing database connections."""
    def __init__(self, db_config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_config = db_config
        self.conn = None

    def connect(self):
        """Establish a database connection."""
        try:
            self.conn = psycopg.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                dbname=self.db_config.database,
                user=self.db_config.username,
                password=self.db_config.password
            )
        except Exception as error:
            self.logger.error(f"Error connecting to the database: {error}")
            raise

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def execute_query(self, query, params=None):
        """Execute a query with optional parameters."""
        if not self.conn:
            self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None

    def commit(self):
        """Commit changes to the database."""
        if self.conn:
            self.conn.commit()


class Database:
    def __init__(self, config: Dynaconf, test=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_handler = DatabaseHandler(config.db.dev if not test else config.db.test)
        self.db_params = config.db.params

    def create_database(self):
        """Ensure the database exists, creating it if necessary."""
        try:
            with psycopg.connect(
                host=self.db_handler.db_config.host,
                port=self.db_handler.db_config.port,
                dbname="postgres",
                user=self.db_handler.db_config.username,
                password=self.db_handler.db_config.password
            ) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(sql.SQL(CHECK_DB_EXISTENCE).format(sql.Literal(self.db_handler.db_config.database)))
                    if not cur.fetchone():
                        cur.execute(sql.SQL(CREATE_DB).format(sql.Identifier(self.db_handler.db_config.database)))
                        self.logger.info(f"Database '{self.db_handler.db_config.database}' created successfully.")
                    else:
                        self.logger.info(f"Database '{self.db_handler.db_config.database}' already exists.")
        except Exception as error:
            self.logger.error(f"Error creating database: {error}")
            raise

    def create_tables(self):
        """Create required tables in the database if they do not exist."""
        self.db_handler.connect()
        with self.db_handler.conn.cursor() as cur:
            self.execute_nested_query_structure(cur, create_source_schema)
            self.execute_nested_query_structure(cur, create_price_map_schema)
            if not self.db_handler.db_config.database == 'test_db':
                cur.execute(
                    sql.SQL(RESET_SEQUENCE).format(
                        sql.Identifier('report_batches_id_seq'),
                        sql.Literal(self.db_params.report_batch_id)
                    )
                )
        self.db_handler.commit()
        self.logger.info("Tables created successfully.")

    def execute_nested_query_structure(self, cursor, query_structure: Dict[str, Union[str, Dict[str, str]]]):
        """Execute a nested structure of SQL queries."""
        for table_name, queries in query_structure.items():
            if isinstance(queries, str):
                cursor.execute(queries)
            elif isinstance(queries, dict):
                for query_name, query in queries.items():
                    cursor.execute(query)
            else:
                raise TypeError(f"Invalid query structure for '{table_name}': {type(queries).__name__}")

    def initiate_db(self):
        """Initialize the database, ensuring it exists and creating necessary tables."""
        self.create_database()
        self.create_tables()

    def get_cached_geoid(self, geo_index: List[str]):
        """Retrieve cached geo_id for a given zip code."""
        query = REFLECT_AVIVID % ', '.join('%s' for _ in geo_index)
        result = self.db_handler.execute_query(query, geo_index)
        return [row[0] for row in result] if result else None

    def cache_geo_response(self, geocoding_response: GeocodingResponse):
        """Cache geocoding response data in the geo_cache table."""
        query = insert_source['geo_cache']
        geocoding_data = (
            geocoding_response.geo_index,
            geocoding_response.hd_geo_id,
            geocoding_response.id,
            geocoding_response.type_key,
            json.dumps(geocoding_response.coordinates),
            geocoding_response.match_name,
            geocoding_response.confidence_score
        )
        self.db_handler.execute_query(query, geocoding_data)
        self.db_handler.commit()

    def get_validated_price(self, price_date: str):
        """Retrieve validated price data."""
        query = sql.SQL(VALIDATE_PRICE_GEN).format(sql.Literal(price_date))
        result = self.db_handler.execute_query(query)
        return [row[0] for row in result] if result else None

    def store_price_in_db(self, price_response: PriceResponse):
        """Store price response data in the prices_all table."""
        if price_response:
            query = insert_source['prices_all']
            price_data = (
                price_response.place_id,
                price_response.price_date,
                price_response.transaction_type,
                json.dumps(price_response.house_price),
                json.dumps(price_response.apartment_price),
                json.dumps(price_response.hybrid_price)
            )
            self.db_handler.execute_query(query, price_data)
            self.db_handler.commit()

    def get_last_value_sequence(self):
        """Retrieve the last value of a sequence."""
        query = sql.SQL(GET_SEQUENCE_VALUE).format(sql.Identifier('report_batches_id_seq'))
        result = self.db_handler.execute_query(query)
        return result[0][0] if result else None
