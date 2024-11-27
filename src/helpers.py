import boto3
import json
import psycopg
from typing import List, Dict
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from src.db import Database


class S3Connector:
    def __init__(self, bucket_name: str, profile_name: str = "default"):
        """
        Initialize the S3Connector with a bucket name and AWS profile.
        :param bucket_name: The name of the S3 bucket.
        :param profile_name: The AWS profile name to use (default: 'default').
        """
        self.bucket_name = bucket_name
        self.session = boto3.Session(profile_name=profile_name)
        self.s3_client = self.session.client("s3")

    def upload_json(self, file_path: str, s3_key: str):
        """
        Upload a JSON file to the specified S3 bucket.
        :param file_path: The local path of the JSON file to upload.
        :param s3_key: The S3 key (object name) to use for the uploaded file.
        """
        try:
            with open(file_path, "r") as json_file:
                data = json.load(json_file)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=json.dumps(data),
                    ContentType="application/json"
                )
            print(f"Successfully uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
        except FileNotFoundError:
            print(f"Error: File {file_path} not found.")
        except json.JSONDecodeError:
            print(f"Error: File {file_path} is not valid JSON.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: AWS credentials issue - {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def upload_json_data(self, json_data: Dict, s3_key: str):
        """
        Upload a JSON object directly to the specified S3 bucket.
        :param json_data: The JSON object to upload.
        :param s3_key: The S3 key (object name) to use for the uploaded file.
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(json_data),
                ContentType="application/json"
            )
            print(f"Successfully uploaded JSON data to s3://{self.bucket_name}/{s3_key}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: AWS credentials issue - {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


class PostgresToS3(Database):
    def __init__(self, config: Dict, s3_connector: S3Connector, test: bool):
        """
        Initialize the PostgresToS3 class.
        :param db_config: Database connection parameters.
        :param s3_connector: An instance of S3Connector for S3 operations.
        """
        super().__init__(config=config, test=test)
        self.s3_connector = s3_connector

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
            print(f"Error dumping table {table_name} to JSON: {e}")
            return []

    def dump_and_upload(self, table_name: str, s3_key: str):
        """
        Dump a PostgreSQL table to JSON and upload it to S3.
        :param table_name: The name of the PostgreSQL table.
        :param s3_key: The S3 key (object name) for the uploaded JSON file.
        """
        print(f"Dumping table '{table_name}' to JSON...")
        table_data = self.dump_table_to_json(table_name)
        if table_data:
            print(f"Uploading table data to S3 bucket: {self.s3_connector.bucket_name}, key: {s3_key}")
            self.s3_connector.upload_json_data(table_data, s3_key)
        else:
            print(f"No data to upload for table {table_name}.")

    def save_json_to_file(self, data: List[Dict], file_path: str):
        """
        Save JSON data to a local file.
        :param data: The JSON data to save (list of dictionaries).
        :param file_path: The path of the file to save the JSON data to.
        """
        try:
            with open(file_path, "w") as json_file:
                json.dump(data, json_file, indent=4)
            print(f"Data successfully saved to {file_path}")
        except Exception as e:
            print(f"Error saving JSON to file {file_path}: {e}")
