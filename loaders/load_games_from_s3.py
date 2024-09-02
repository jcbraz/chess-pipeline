import logging
from typing import Union
from botocore.utils import ClientError
from dataclasses import dataclass
from utils.S3Bucket import S3Bucket
from pyspark.sql import DataFrame, SparkSession
# import spark exception
from pyspark.errors import PySparkException

@dataclass
class LoaderGamesFromS3:
    object_key: str
    s3_access_key: str
    s3_secret_key: str
    bucket_name: str
    endpoint: str

    def __post_init__(self):
        self.spark = SparkSession.builder \
            .appName("ChessGamesReader") \
            .getOrCreate()

        spark_context = self.spark.sparkContext
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.s3_access_key)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.s3_secret_key)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", self.endpoint)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")

    def _load_games_object(self) -> DataFrame:
        """
        Load an object from the bucket.

        :param object_key: The key of the object to load.
        :return: The object data.
        """
        try:
            # https://chess-pipeline.s3.cubbit.eu/lichess_standard_rated_2021-08.pgn.zst
            url_to_hit = f"https://{self.bucket_name}.{self.endpoint}/{self.object_key}"
            games_df = self.spark.read.text(f"s3a://{self.bucket_name}/{self.object_key}", header=True)
        except PySparkException as e:
            raise FileNotFoundError(f"Object '{self.object_key}' not found in bucket '{self.bucket_name}'.")

        return games_df

    def parse_games(self) -> Union[DataFrame, None]:
        """
        Parse the games from the bucket.
        """
        try:
            games_df = self._load_games_object()
            return games_df
        except BaseException as e:
            logging.error(f"An error occurred while parsing the games: {str(e)}")
            return None
