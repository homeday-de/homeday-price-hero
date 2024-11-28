import boto3
import json
from dynaconf import Dynaconf
from typing import Dict
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class S3Connector:
    def __init__(self, config: Dynaconf, profile_name: str = "default"):
        """
        Initialize the S3Connector with a bucket name and AWS profile.
        :param config: config file.
        :param profile_name: The AWS profile name to use (default: 'default').
        """
        self.bucket_name = config.aws.s3_bucket
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
