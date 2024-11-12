import pytest
import json
import configparser
from src.db import Database


config = configparser.ConfigParser()
config.read('config/config.dev.ini')
db = Database(config=config, test=True)


# Fixture for database connection setup and teardown
@pytest.fixture
def db_conn():
    """Fixture to set up the database connection and tables for testing."""
    # Connect to test database
    db.connect_to_db()
    db.create_tables()
    yield db.conn
    # Teardown: Close connection and clean up
    db.conn.close()

# Test connection function
def test_connect_to_db(db_conn):
    assert db_conn is not None, "Database connection should be established"

# Test create_tables function
def test_create_tables(db_conn):
    with db_conn.cursor() as cur:
        """Test that the tables are created successfully."""
        # Check if tables exist
        cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'prices_all');")
        assert cur.fetchone()[0] is True

        cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'geo_cache');")
        assert cur.fetchone()[0] is True

# Test cache_geoid function
def test_cache_geoid(db_conn):
    zip_code = "67890"
    geocoding_response = {
        "id": "geo456",
        "type_key": "NBH2",
        "coordinates": json.dumps({"lat": 52.503, "lng": 13.518}),
        "match_name": "SampleName",
        "confidence_score": 1
    }
    db.conn = db_conn
    db.cache_geoid(zip_code, geocoding_response)
    
    cached_geo_id = db.get_cached_geoid(zip_code)
    assert cached_geo_id == geocoding_response["id"]

# Test store_data_in_db function
def test_store_data_in_db(db_conn):
    price_response = {
        "place_id": "geo789",
        "price_date": "2023-10-01",
        "transaction_type": "sell",
        "house_price": json.dumps({"value": 5000}),
        "apartment_price": json.dumps({"value": 3000}),
        "hybrid_price": json.dumps({"value": 4000})
    }
    db.conn = db_conn
    db.store_data_in_db(price_response)

    with db.conn.cursor() as cur:
        cur.execute("SELECT * FROM prices_all WHERE geo_id = 'geo789'")
        result = cur.fetchone()
    
    assert result is not None, "Data should be stored in the database"
    assert result[1] == 'geo789', f"Expected 'geo789', but got {result[1]}"
    assert result[2] == '2023-10-01', f"Expected '2023-10-01', but got {result[2]}"
