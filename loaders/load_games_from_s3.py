import logging
import os
from typing import Union
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import PySparkException


@dataclass
class LoaderGamesFromS3:
    object_key: str
    s3_access_key: str
    s3_secret_key: str
    region: str
    bucket_name: str
    endpoint: str

    def __post_init__(self):
        self.spark = SparkSession.builder.appName("ChessGamesReader").getOrCreate()

        self.spark.conf.set("fs.s3a.access.key", self.s3_access_key)
        self.spark.conf.set("fs.s3a.secret.key", self.s3_secret_key)
        self.spark.conf.set("fs.s3a.endpoint", self.endpoint)

    def load_games_dataframe_from_s3(self) -> Union[DataFrame, None]:
        """
        Load an object from the bucket.

        :param object_key: The key of the object to load.
        :return: The object data.
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
