import logging
import os
from typing import Union
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import PySparkException


@dataclass
class S3Bucket:
    spark: SparkSession
    object_key: str
    s3_access_key: str
    s3_secret_key: str
    region: str
    bucket_name: str
    endpoint: str

    def load_games_dataframe_from_s3(self) -> Union[DataFrame, None]:
        """
        Load an object from the bucket.

        :param self: class self
        """
        try:
            logging.info(
                "Loading object '%s' from bucket '%s'.",
                self.object_key,
                self.bucket_name,
            )
            url_to_hit = f"s3a://{self.bucket_name}/{self.object_key}"

            try:
                games_df = self.spark.read.text(url_to_hit)
                return games_df
            except PySparkException as e:
                logging.error(
                    "An error occurred while reading the object with Spark: %s", str(e)
                )
                return None

        except PySparkException as e:
            logging.error("An error occurred while loading the object: %s", str(e))
            return None

    def load_games_to_s3(self, df_to_upload: DataFrame, bucket_key: str) -> None:
        """
        Load formatted games back into S3 Bucket

        :param self: class self
        """
        try:
            logging.info("Loading Dataframe to bucket '%s", self.bucket_name)

            bucket_url_to_hit = f"s3a://{self.bucket_name}/{bucket_key}.parquet"
            df_to_upload.write.parquet(path=bucket_url_to_hit, mode="overwrite")

        except PySparkException as e:
            logging.error(
                "An error ocurred uploading dataframe to bucket '%s", e.message
            )
