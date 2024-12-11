import boto3
import json
import base64
import os
from dynaconf import Dynaconf
from typing import Dict
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


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


class SecretManager:

    region_name = "eu-central-1"
    secret_path = os.getenv("SECRET_PATH", 'config/.secrets.json')

    def get_secret(self, secret_name):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=self.region_name)

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in get_secret_value_response:
                return get_secret_value_response["SecretString"]
            else:
                return base64.b64decode(get_secret_value_response["SecretBinary"])

    def update_secret(self, secret_name):

        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=self.region_name)
        with open(self.secret_path, 'r') as file:
            data = json.load(file)
        json_string = json.dumps(data, indent=4)

        try:
            update_secret_value_response = client.update_secret(SecretId=secret_name, SecretString=json_string)
            print(update_secret_value_response)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            
    def create_config_file(self, secret_name: str) -> Dict:
        secret_path = os.path.abspath(f"{self.secret_path}")
        if os.path.exists(secret_path):
            os.remove(secret_path)
        secret_value = self.get_secret(f"homeday-prices-lake/{secret_name}")
        secret = json.loads(secret_value)
        
        with open(self.secret_path, "w") as f:
            json.dump(secret, f, indent=4)

    def update_secret_to_vault(self, secret_name: str):
        secret_path = os.path.abspath(f"{self.secret_path}")
        if not os.path.exists(secret_path):
            raise FileNotFoundError("Local config file doesn't exist")
        
        return self.update_secret(f"homeday-prices-lake/{secret_name}")
