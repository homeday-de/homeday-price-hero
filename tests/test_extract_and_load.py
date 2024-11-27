import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from src.lib.aws import S3Connector
from src.pipelines.extract_and_load import APIToPostgres, PostgresToS3
from src.models import PriceResponse


class TestFixtures:
    """Class to provide shared fixtures."""
    config = {
        'api.dev': {
            'geo_coding_url': 'http://geo.dev/api',
            'price_url': 'http://price.dev/api',
            'geo_api_key': 'fake_geo_key',
            'price_api_key': 'fake_price_key',
        },
        'aws': {
            's3_bucket': 'test_bucket'
        }
    }

    @pytest.fixture
    def s3_connector(self):
        """Fixture to initialize the S3Connector with mock configuration."""
        with patch("src.lib.aws.boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client
            connector = S3Connector(config=self.config, profile_name="default")
            yield connector

    @pytest.fixture
    def postgres_to_s3(self, s3_connector):
        """Fixture to initialize PostgresToS3 with mock dependencies."""
        with patch("src.pipelines.extract_and_load.Database.connect_to_db") as mock_connect_to_db:
            mock_connect_to_db.return_value = MagicMock()  # Mock database connection
            # Mock the connection and cursor
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect_to_db.return_value = mock_conn
            pg_to_s3 = PostgresToS3(config=self.config, s3_connector=s3_connector, test=True)
            pg_to_s3.conn = mock_conn
            yield pg_to_s3

    @pytest.fixture
    def mock_api_to_postgres(self):
        """Fixture to provide a mocked instance of APIToPostgres."""
        return APIToPostgres(self.config, test=True)


class TestPostgresToS3(TestFixtures):
    """Test suite for the PostgresToS3 class."""

    def test_upload_json_to_s3(self, s3_connector):
        """Test the upload_json_data method of S3Connector."""
        json_data = {"key": "value"}
        s3_key = "test_key.json"
        bucket_name = self.config.get('aws', 's3_bucket')

        s3_connector.upload_json_data(json_data, s3_key)
        s3_connector.s3_client.put_object.assert_called_once_with(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(json_data),
            ContentType="application/json",
        )

    def test_dump_table_to_json(self, postgres_to_s3):
        """Test the dump_table_to_json method."""
        table_name = "test_table"

        # Set up mock cursor data
        postgres_to_s3.conn.cursor.return_value.__enter__.return_value.description = [
            ("column1",), ("column2",)
        ]
        postgres_to_s3.conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
            ("value1", "value2"), ("value3", "value4")
        ]

        result = postgres_to_s3.dump_table_to_json(table_name)
        assert result == [
            {"column1": "value1", "column2": "value2"},
            {"column1": "value3", "column2": "value4"},
        ]

    def test_dump_and_upload(self, postgres_to_s3):
        """Test the dump_and_upload method."""
        table_name = "test_table"
        s3_key = "test_key.json"

        # Mock dump_table_to_json to return fake table data
        with patch.object(postgres_to_s3, "dump_table_to_json", return_value=[{"key": "value"}]) as mock_dump, \
             patch.object(postgres_to_s3.s3_connector, "upload_json_data") as mock_upload:
            
            postgres_to_s3.dump_and_upload(table_name, s3_key)
            mock_dump.assert_called_once_with(table_name)
            mock_upload.assert_called_once_with([{"key": "value"}], s3_key)

    def test_save_json_to_file(self, postgres_to_s3, tmp_path):
        """Test the save_json_to_file method."""
        file_path = tmp_path / "test_file.json"
        data = [{"key": "value"}]

        postgres_to_s3.save_json_to_file(data, file_path)

        with open(file_path, "r") as file:
            saved_data = json.load(file)

        assert saved_data == data


class TestAPIToPostgres(TestFixtures):
    """Test suite for the APIToPostgres class."""

    @pytest.mark.asyncio
    async def test_fetch_price(self, mock_api_to_postgres):
        """Test the fetch_price method of APIToPostgres."""
        zip_codes = ["12345", "67890"]
        price_date = "2024-01-01"

        # Mock APIClient and its methods
        mock_api_client = MagicMock()
        mock_api_client.get_data_in_batch = AsyncMock(return_value=[
            PriceResponse(
                place_id="place_123",
                price_date="2024-01-01",
                transaction_type="sale",
                house_price={"median": 300000, "average": 310000},
                apartment_price={"median": 200000, "average": 210000},
                hybrid_price={"median": 250000, "average": 260000}
            ),
            PriceResponse(
                place_id="place_456",
                price_date="2024-01-01",
                transaction_type="rent",
                house_price={"median": 1000, "average": 1100},
                apartment_price={"median": 800, "average": 900},
                hybrid_price={"median": 900, "average": 950}
            ),
        ])

        # Patch dependent methods
        with patch.object(mock_api_to_postgres, "api_client", return_value=mock_api_client), \
             patch.object(mock_api_to_postgres, "ensure_geoid_cache", new_callable=AsyncMock) as mock_ensure_cache:

            # Set up return values
            mock_ensure_cache.return_value = ["place_123", "place_456"]
            mock_api_to_postgres.api = mock_api_client

            # Call the fetch_price method
            result = await mock_api_to_postgres.fetch_price(zip_codes, price_date)

            # Assertions
            mock_ensure_cache.assert_awaited_once_with(zip_codes)
            mock_api_client.get_data_in_batch.assert_awaited_once_with(
                mock_api_to_postgres.PRICE_URL,
                ["place_123", "place_456"],
                mock_api_client.fetch_price_data,
                price_date=price_date
            )
            assert len(result) == 2
            assert isinstance(result[0], PriceResponse)
            assert result[0].place_id == "place_123"
            assert result[0].house_price["median"] == 300000
            assert result[1].transaction_type == "rent"
            assert result[1].apartment_price["average"] == 900
