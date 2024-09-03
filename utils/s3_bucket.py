import logging
import os
from dataclasses import dataclass
from typing import Any
from boto3 import client
from botocore.exceptions import ClientError
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dataclass
class S3Bucket:
    bucket_name: str
    region: str
    access_key_id: str
    secret_access_key: str

    def __post_init__(self):
        self.client = client(
            "s3",
            endpoint_url="https://s3.cubbit.eu",
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

    def get(self, object_key: str):
        """
        Gets the object.

        :return: The object data in bytes.
        """
        try:
            logger.info("Getting object '%s' from bucket '%s'.", object_key, self.bucket_name)
            body = self.client.get_object(Bucket=self.bucket_name, Key=object_key)["Body"].read()
            logger.info(
                "Got object '%s' from bucket '%s'.",
                object_key,
                self.bucket_name,
            )
        except ClientError as e:
            logger.exception(
                "Couldn't get object '%s' from bucket '%s': %s",
                object_key,
                self.bucket_name,
                str(e),
            )
            raise
        else:
            return body


    def put(self, data: Any, object_key: str):
        """
        Upload data to the object.

        :param data: The data to upload. This can either be bytes or a string. When this
                     argument is a string, it is interpreted as a file name, which is
                     opened in read bytes mode.
        """
        put_data = data
        total_size = None

        if isinstance(data, str):
            try:
                put_data = open(data, "rb")
                total_size = os.path.getsize(data)
            except IOError:
                logger.exception(f"Expected file name or binary data, got {data}.",)
                raise

        elif hasattr(data, '__len__'):
            total_size = len(data)

        try:
            logger.info(
                "Uploading object '%s' to bucket '%s'.", object_key, self.bucket_name,
            )

            if total_size is not None:
                pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Uploading {object_key}")

                def upload_progress(bytes_amount: int):
                    pbar.update(bytes_amount)

                self.client.upload_fileobj(put_data, self.bucket_name, object_key, Callback=upload_progress)
                pbar.close()
            else:
                self.client.upload_fileobj(put_data, self.bucket_name, object_key)

            logger.info(
                "Object '%s' successfully uploaded to bucket '%s'.",
                object_key,
                self.bucket_name,
            )
        except ClientError:
            logger.exception(
                "Couldn't upload object '%s' to bucket '%s'.",
                object_key,
                self.bucket_name,
            )
            raise
        finally:
            if getattr(put_data, "close", None):
                put_data.close()


    def check_key_existence(self, object_key: str) -> bool:
        """
        Checks if the object key exists in the bucket.

        :return: True if the object key exists, False otherwise.
        """
        try:
            bucket_objects = self.client.list_objects_v2(Bucket=self.bucket_name)
            if "Contents" in bucket_objects:
                return any(obj["Key"] == object_key for obj in bucket_objects["Contents"])
            return False
        except ClientError:
            logger.exception("Couldn't get S3 resource.")
            return False


# ds3 = S3Bucket(
#     bucket_name="chess-pipeline",
#     access_key_id=os.environ["S3_ACCESS_KEY_ID"],
#     secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
#     region="eu-west-1"
# )

# print(ds3.check_key_existence(object_key="lichess_standard_rated_2024-08.pgn.zst"))
