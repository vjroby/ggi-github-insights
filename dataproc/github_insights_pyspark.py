from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark.sql.types import StructType, StringType


class GGiGitHubInsights:

    def __init__(self, spark_session: SparkSession, csv_filenames_path: str):
        self.spark = spark_session
        self.csv_filenames_path = csv_filenames_path

    def get_json_filenames(self) -> DataFrame:
        schema = StructType() \
            .add('Filename', StringType(), nullable=False)
        return self.spark.read \
            .options(delimiter=',', header='False') \
            .schema(schema) \
            .csv(self.csv_filenames_path)

    @staticmethod
    def convert_to_list(filenames: DataFrame) -> List[str]:
        fns_ls = filenames.collect()
        return list(map(lambda n: n[0], fns_ls))

    def read_files(self, files: List[str], prepend_path='') -> List[DataFrame]:
        return list(map(lambda f: self.read_json_gzipped(f"{prepend_path}/{f}"), files))

    def read_json_gzipped(self, filename: str) -> DataFrame:
        return self.spark.read \
            .json(filename) \
            .where(F.col('type') == 'PushEvent') \
            .select(
                F.col('repo.id').alias('repository_id'),
                F.col('payload.distinct_size').alias('dist_commits'),
                F.to_timestamp(F.col('created_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('timestamp')) \
            .withColumn('week_of_year', F.weekofyear(F.col('timestamp'))) \
            .withColumn('month', F.month(F.col('timestamp'))) \
            .withColumn('day', F.to_date(F.col('timestamp'))) \
            .drop('timestamp')\
            .na.drop()

    def union_dfs(self, events_df: List[DataFrame]) -> DataFrame:
        result = events_df[0]
        for df in events_df[1:]:
            result = result.union(df)
        return result

    @staticmethod
    def group_by_columns(df: DataFrame, cols: List[str]) -> Dict[str, DataFrame]:
        df.cache()
        result = {}
        for col in cols:
            result[col] = df.groupBy(F.col(col)).agg(F.sum(F.col('dist_commits')).alias(f"sum_commits_by_{col}"))
        return result


if __name__ == "__main__":
    pass
