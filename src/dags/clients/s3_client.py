from typing import Dict
from boto3.session import Session


class S3Client:

    def __init__(self, s3_client_info: Dict, path_template: str, bucket: str) -> None:
        self.s3_client = Session().client(**s3_client_info)
        self.path_template = path_template
        self.bucket = bucket

    def fetch_s3_file(self, key: str) -> None:
        self.s3_client.download_file(
            Bucket=self.bucket,
            Key=key,
            Filename=self.path_template.format(key)
        )
