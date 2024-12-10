import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from src.lib.aws import S3Connector
from src.pipelines.extract_and_load import APIToPostgres, PostgresToS3
from src.models import PriceResponse
from config import settings


class TestFixtures:
    """Class to provide shared fixtures."""

    @pytest.fixture
    def s3_connector(self):
        """Fixture to initialize the S3Connector with mock configuration."""
        with patch("src.lib.aws.boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client
            connector = S3Connector(config=settings, profile_name="default")
            yield connector

    @pytest.fixture
    def postgres_to_s3(self, s3_connector):
        """Fixture to initialize PostgresToS3 with mock dependencies."""
        with patch("src.db.database.DatabaseHandler.connect") as mock_connect_to_db:
            mock_connect_to_db.return_value = MagicMock()  # Mock database connection
            # Mock the connection and cursor
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect_to_db.return_value = mock_conn
            pg_to_s3 = PostgresToS3(config=settings, s3_connector=s3_connector, test=True)
            pg_to_s3.conn = mock_conn
            yield pg_to_s3

    @pytest.fixture
    def mock_api_to_postgres(self):
        """Fixture to provide a mocked instance of APIToPostgres."""
        return APIToPostgres(settings, test=True)


class TestPostgresToS3(TestFixtures):
    """Test suite for the PostgresToS3 class."""

    def test_upload_json_to_s3(self, s3_connector):
        """Test the upload_json_data method of S3Connector."""
        json_data = {"key": "value"}
        s3_key = "test_key.json"
        bucket_name = settings.aws.s3_bucket

        s3_connector.upload_json_data(json_data, s3_key)
        s3_connector.s3_client.put_object.assert_called_once_with(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(json_data),
            ContentType="application/json",
        )

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
    async def test_fetch_with_retry(self, mock_api_to_postgres):
        mock_fetch_function = AsyncMock(side_effect=[Exception("Error"), {"data": "success"}])
        result = await mock_api_to_postgres.fetch_with_retry(
            base_url="http://example.com", unit={"id": 1}, fetch_function=mock_fetch_function
        )
        assert result == {"data": "success"}
        assert mock_fetch_function.call_count == 2  # First call fails, second succeeds


    @pytest.mark.asyncio
    async def test_process_data_in_batch(self, mock_api_to_postgres):
        """Test the process_data_in_batch method."""
        mock_fetch_function = AsyncMock()
        mock_cache_function = MagicMock()
        idx_group = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        batch_size = 2
        rate_limit_interval = 0.1

        # Mock fetch function to return the index as a result
        mock_fetch_function.side_effect = lambda base_url, unit, **kwargs: {"data": unit}
        mock_cache_function.side_effect = lambda result: None

        with patch.object(mock_api_to_postgres, "logger") as mock_logger:
            await mock_api_to_postgres.process_data_in_batch(
                base_url="http://example.com",
                idx_group=idx_group,
                fetch_function=mock_fetch_function,
                cache_function=mock_cache_function,
                batch_size=batch_size,
                rate_limit_interval=rate_limit_interval,
            )

            # Assertions
            assert mock_fetch_function.call_count == len(idx_group)
            assert mock_cache_function.call_count == len(idx_group)
            mock_logger.info.assert_called()


    @pytest.mark.asyncio
    async def test_run(self, mock_api_to_postgres):
        """Test the run method."""
        mock_api_to_postgres.api = MagicMock()
        mock_api_to_postgres.ensure_geoid_cache = AsyncMock()
        mock_api_to_postgres.fetch_price = AsyncMock()
        mock_api_to_postgres.db_handler.close = MagicMock()

        geo_indices = {"zip_codes": [{"name": "12345"}], "cities": [{"name": "Berlin"}]}
        price_date = "2024-01-01"

        await mock_api_to_postgres.run(geo_indices, price_date)

        # Assertions
        mock_api_to_postgres.ensure_geoid_cache.assert_awaited_once_with(geo_indices)
        mock_api_to_postgres.fetch_price.assert_awaited_once_with(price_date)
        mock_api_to_postgres.db_handler.close.assert_called_once()


    @pytest.mark.asyncio
    async def test_fetch_price(self, mock_api_to_postgres):
        """Test the fetch_price method."""
        mock_api_to_postgres.api = MagicMock()
        mock_api_to_postgres.get_validated_price = MagicMock(return_value=["geoid_1", "geoid_2"])
        mock_api_to_postgres.process_data_in_batch = AsyncMock()

        price_date = "2024-01-01"

        await mock_api_to_postgres.fetch_price(price_date)

        # Assertions
        mock_api_to_postgres.get_validated_price.assert_called_once_with(price_date)
        mock_api_to_postgres.process_data_in_batch.assert_awaited_once_with(
            mock_api_to_postgres.PRICE_URL,
            ["geoid_1", "geoid_2"],
            mock_api_to_postgres.api.fetch_price_data,
            mock_api_to_postgres.store_price_in_db,
            mock_api_to_postgres.api.batch_size,
            mock_api_to_postgres.api.rate_limit_interval,
            price_date=price_date,
        )


    @pytest.mark.asyncio
    async def test_ensure_geoid_cache(self, mock_api_to_postgres):
        """Test the ensure_geoid_cache method."""
        mock_api_to_postgres.get_cached_geoid = MagicMock(side_effect=lambda names: None if names[0] == "12345" else ["geoid_456"])
        mock_api_to_postgres.fetch_geo = AsyncMock()

        geo_indices = {
            "zip_codes": [{"name": "12345"}, {"name": "67890"}],
            "cities": [{"name": "Berlin"}]
        }

        await mock_api_to_postgres.ensure_geoid_cache(geo_indices)

        # Assertions
        mock_api_to_postgres.get_cached_geoid.assert_any_call(["12345"])
        mock_api_to_postgres.get_cached_geoid.assert_any_call(["67890"])
        mock_api_to_postgres.get_cached_geoid.assert_any_call(["Berlin"])
        mock_api_to_postgres.fetch_geo.assert_awaited_once_with([{"name": "12345"}])


    @pytest.mark.asyncio
    async def test_fetch_geo(self, mock_api_to_postgres):
        """Test the fetch_geo method."""
        mock_api_to_postgres.api = MagicMock()
        mock_api_to_postgres.process_data_in_batch = AsyncMock()

        index_group = [{"name": "Berlin"}, {"name": "Hamburg"}]

        await mock_api_to_postgres.fetch_geo(index_group)

        # Assertions
        mock_api_to_postgres.process_data_in_batch.assert_awaited_once_with(
            mock_api_to_postgres.GEOCODING_URL,
            index_group,
            mock_api_to_postgres.api.fetch_geocoding_data,
            mock_api_to_postgres.cache_geo_response,
            mock_api_to_postgres.api.batch_size,
            mock_api_to_postgres.api.rate_limit_interval,
        )