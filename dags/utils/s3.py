from contextlib import contextmanager

import boto3


class S3Utils:

    def __init__(
            self,
            host,
            bucket_name,
            access_key,
            secret_key,
            ):
        self.host = host
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key

    @contextmanager
    def connect(
            self,
    ):
        session = boto3.session.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        s3 = session.resource(
            's3',
            endpoint_url=self.host,
        )
        bucket = s3.Bucket(self.bucket_name)
        yield bucket

    def get(self, remote_file_name: str, local_file_name: str):
        with self.connect() as bucket:
            bucket.download_file(remote_file_name, local_file_name)

    def put(self, local_file_name: str, remote_file_name: str):
        with self.connect() as bucket:
            bucket.upload_file(local_file_name, remote_file_name)

    def ls(self):
        with self.connect() as bucket:
            return [
                obj
                for obj in bucket.objects.all()
            ]