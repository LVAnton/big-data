import os
import io
from abc import ABC, abstractmethod
import boto3
from botocore.client import Config


class MinioInterface(ABC):
    @abstractmethod
    def upload_bytes(self, data: bytes, bucket: str, key: str) -> None:
        ...

    @abstractmethod
    def bucket_exists(self, bucket: str) -> bool:
        ...

    @abstractmethod
    def create_bucket(self, bucket: str) -> None:
        ...


class MinioBoto3(MinioInterface):
    def __init__(self):
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ROOT_USER", "minio")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )

    def upload_bytes(self, data: bytes, bucket: str, key: str) -> None:
        self.client.upload_fileobj(io.BytesIO(data), bucket, key)

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except self.client.exceptions.ClientError:
            return False

    def create_bucket(self, bucket: str) -> None:
        self.client.create_bucket(Bucket=bucket)


def get_minio_client() -> MinioInterface:
    return MinioBoto3()