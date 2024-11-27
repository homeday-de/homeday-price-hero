import pytest
import json
from unittest.mock import patch, MagicMock
from src.helpers import S3Connector, PostgresToS3


@pytest.fixture
def s3_connector():
    """Fixture to initialize the S3Connector with mock configuration."""
    with patch("src.helpers.boto3.Session") as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        connector = S3Connector(bucket_name="hd-prices-lake", profile_name="default")
        yield connector


@pytest.fixture
def postgres_to_s3(s3_connector):
    """Fixture to initialize PostgresToS3 with mock dependencies."""
    with patch("src.helpers.Database.connect_to_db") as mock_connect_to_db:
        mock_connect_to_db.return_value = MagicMock()  # Mock database connection
        import configparser
        config = configparser.ConfigParser()
        config.read('config/config.dev.ini')
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Set the cursor to return mock data
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        # Patch the connection to return our mock connection
        mock_connect_to_db.return_value = mock_conn
        # Initialize the PostgresToS3 instance
        pg_to_s3 = PostgresToS3(config=config, s3_connector=s3_connector, test=True)
        # Set the mocked connection to the instance
        pg_to_s3.conn = mock_conn

        yield pg_to_s3


def test_upload_json_to_s3(s3_connector):
    """Test the upload_json_data method of S3Connector."""
    json_data = {"key": "value"}
    s3_key = "test_key.json"

    s3_connector.upload_json_data(json_data, s3_key)
    s3_connector.s3_client.put_object.assert_called_once_with(
        Bucket="hd-prices-lake",
        Key=s3_key,
        Body=json.dumps(json_data),
        ContentType="application/json",
    )


def test_dump_table_to_json(postgres_to_s3):
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


def test_dump_and_upload(postgres_to_s3):
    """Test the dump_and_upload method."""
    table_name = "test_table"
    s3_key = "test_key.json"

    # Mock dump_table_to_json to return fake table data
    with patch.object(postgres_to_s3, "dump_table_to_json", return_value=[{"key": "value"}]) as mock_dump, \
        patch.object(postgres_to_s3.s3_connector, "upload_json_data") as mock_upload:
        
        postgres_to_s3.dump_and_upload(table_name, s3_key)
        mock_dump.assert_called_once_with(table_name)
        mock_upload.assert_called_once_with([{"key": "value"}], s3_key)


def test_save_json_to_file(postgres_to_s3, tmp_path):
    """Test the save_json_to_file method."""
    file_path = tmp_path / "test_file.json"
    data = [{"key": "value"}]

    postgres_to_s3.save_json_to_file(data, file_path)

    with open(file_path, "r") as file:
        saved_data = json.load(file)

    assert saved_data == data
