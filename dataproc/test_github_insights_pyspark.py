from dataproc.start_pyspak import GGiGitHubInsights
from pyspark.sql import SparkSession, DataFrame
from pandas.testing import assert_frame_equal
from pyspark.sql import functions as F
from typing import List

import logging
import numpy
import pandas as pd
import unittest


class GGiGitHubInsightsTest(unittest.TestCase):
    spark: SparkSession = None
    base_path = 'dataproc/testdata/'

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder
                     .master('local[2]')
                     .appName('ggi-test-app')
                     .getOrCreate())

    @staticmethod
    def supress_py4j_logging():
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    def test_get_json_filenames(self):
        ggi = self.create_ggi_insights('ggi_files_to_process')

        expected = pd.DataFrame({'Filename': ['2017-01-01-0.json.gz', '2017-03-01-0.json.gz']})
        df = ggi.get_json_filenames()
        self.assert_frame_equal_with_sort(df, expected, 'Filename')

    def test_read_json_gzipped(self):
        ggi = self.create_ggi_insights('')
        expected = pd.DataFrame(self.create_pd_df([
            '22484898|2|2017-03-01 00:00:00|9|3',
            '69984295|1|2017-03-01 00:00:00|9|3',
            '80294987|2|2017-03-01 00:00:00|9|3',
            '82382846|1|2017-03-01 00:00:00|9|3',
            '35569859|0|2017-03-01 00:00:01|9|3'
        ])).astype({'month': 'int32','week_of_year': 'int32'})
        df = ggi.read_json_gzipped(f'{self.base_path}/2017-03-01-0.json.gz') \
            .sort('timestamp', 'repository_id') \
            .limit(5)

        self.assert_frame_equal_with_sort(df, expected, ['timestamp', 'repository_id'])

    def create_pd_df(self, values: List[str]):
        def transform(s: str):
            sarr = s.split('|')
            return {'repository_id': numpy.int64(sarr[0]), 'dist_commits': numpy.int64(sarr[1]),
                    'timestamp': numpy.datetime64((sarr[2])), 'week_of_year': int(sarr[3]),
                    'month': int(sarr[4])}
        return list(map(lambda v: transform(v), values))

    @staticmethod
    def assert_frame_equal_with_sort(results: DataFrame, expected: pd.DataFrame, key_columns):
        results_sorted = results.toPandas().sort_values(by=key_columns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=key_columns).reset_index(drop=True)
        assert_frame_equal(results_sorted, expected_sorted)

    def create_ggi_insights(self, csv='ggi_files_to_process.csv') -> GGiGitHubInsights:
        return GGiGitHubInsights(self.spark, f'{self.base_path}/{csv}.csv')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()