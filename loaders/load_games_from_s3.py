import logging
import os
from typing import Union
from botocore.utils import ClientError
from dataclasses import dataclass
from utils import s3_bucket
from utils.s3_bucket import S3Bucket
from utils.zst_to_chunk_files import zst_to_chunk_files
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

    def _load_games_dataframe_from_s3(self) -> Union[DataFrame, None]:
        """
        Load an object from the bucket.

        :param object_key: The key of the object to load.
        :return: The object data.
        """
        try:
            logging.info(
                f"Loading object '{self.object_key}' from bucket '{self.bucket_name}'."
            )
            url_to_hit = f"s3a://{self.bucket_name}/{self.object_key}"
            s3_bucket = S3Bucket(
                bucket_name=self.bucket_name,
                region=self.region,
                access_key_id=self.s3_access_key,
                secret_access_key=self.s3_secret_key,
            )

            try:
                games_df = self.spark.read.text(url_to_hit)
                return games_df
            except BaseException as e:
                logging.error(
                    f"An error occurred while reading the object with Spark: {str(e)}"
                )
                return None

        except PySparkException as e:
            logging.error(f"An error occurred while loading the object: {str(e)}")
            return None

    def parse_games(self) -> Union[DataFrame, None]:
        """
        Parse the games from the bucket.
        """
        try:
            games_df = self._load_games_dataframe_from_s3()
            if not games_df or games_df.isEmpty():
                raise PySparkException("No games found in the object.")

            return games_df
        except BaseException as e:
            logging.error(f"An error occurred while parsing the games: {str(e)}")
            return None


loader = LoaderGamesFromS3(
    object_key="lichess_standard_rated_2024-08.pgn.zst",
    s3_access_key=os.environ["S3_ACCESS_KEY_ID"],
    s3_secret_key=os.environ["S3_SECRET_ACCESS_KEY"],
    region="eu-west-1",
    bucket_name="chess-pipeline",
    endpoint="s3.cubbit.eu",
)

games = loader._load_games_dataframe_from_s3()
if games:
    print(games.limit(5))
