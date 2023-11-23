import boto3
from io import BytesIO

from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


class S3ObjectLoader:
    service_name = 's3'

    def __init__(self, bucket_name, region_name):
        self.region_name = region_name
        self.bucket_name = bucket_name

    @property
    def s3client(self):
        return boto3.client(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

    def load(self, object_name: str) -> BytesIO:
        s3_obj = self.s3client.get_object(Bucket=self.bucket_name, Key=object_name)
        byte_content = s3_obj['Body'].read()

        return BytesIO(byte_content)
