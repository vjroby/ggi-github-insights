from pyspark.sql import SparkSession, DataFrame
from pandas.testing import assert_frame_equal
from typing import List
from pyspark.sql import functions as F
from datetime import datetime,date

import logging
import numpy
import pandas as pd
import unittest

from dataproc.github_insights_pyspark import GGiGitHubInsights


class GGiGitHubInsightsTest(unittest.TestCase):
    # TODO test group by method
    spark: SparkSession = None
    base_path = 'dataproc/testdata/'

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (SparkSession.builder
                     .master('local[2]')
                     .appName('ggi-test-app')
                     .getOrCreate())

    @staticmethod
    def suppress_py4j_logging():
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    def test_get_json_filenames(self):
        ggi = self.create_ggi_insights('ggi_files_to_process')

        expected = pd.DataFrame({'Filename': ['2017-01-01-0.json.gz', '2017-03-01-0.json.gz']})
        df = ggi.get_json_filenames()
        self.assert_frame_equal_with_sort(df, expected, 'Filename')

    def test_read_json_gzipped(self):
        sort_cols =['day', 'repository_id']
        ggi = self.create_ggi_insights('')
        expected = self.create_pd_df_date_strig([
            '25545|0|2017-03-01|9|3',
            '25545|0|2017-03-01|9|3',
            '25550|0|2017-03-01|9|3',
            '25550|0|2017-03-01|9|3',
            '29803|1|2017-03-01|9|3'
        ]).astype({'month': 'int32', 'week_of_year': 'int32','day': '|S'}) \
            .sort_values(by=sort_cols).reset_index(drop=True)
        results = ggi.read_json_gzipped(f'{self.base_path}/2017-03-01-0.json.gz') \
            .sort('day', 'repository_id') \
            .limit(5) \
            .toPandas().astype({'month': 'int32', 'week_of_year': 'int32','day': '|S'}) \
            .sort_values(by=sort_cols).reset_index(drop=True)
        assert_frame_equal(results, expected, check_dtype=False)

    def test_union_dfs(self):
        input1 = self.create_df([
            '22484898|2|2017-03-01|9|3',
            '69984295|1|2017-05-01|9|5',
            '35569859|0|2017-06-01|9|6'])
        input2 = self.create_df(['80294987|2|2017-06-01|9|6'])
        input3 = self.create_df(['82382846|1|2017-08-01|9|8'])
        expected = self.create_pd_df([
            '22484898|2|2017-03-01|9|3',
            '35569859|0|2017-06-01|9|6',
            '69984295|1|2017-05-01|9|5',
            '80294987|2|2017-06-01|9|6',
            '82382846|1|2017-08-01|9|8'
        ])
        ggi = self.create_ggi_insights('')
        result = ggi.union_dfs([input1, input2, input3]).select(
            F.col('repository_id'),
            F.col('dist_commits'),
            F.col('week_of_year'),
            F.col('month'),
            F.col('day'),
        )
        self.assert_frame_equal_with_sort(result, expected, ['repository_id'])

    def create_pd_df_date_strig(self, values: List[str]) -> pd.DataFrame:
        def transform(s: str):
            sarr = s.split('|')
            return {'repository_id': numpy.int64(sarr[0]), 'dist_commits': numpy.int64(sarr[1]),
                    'week_of_year': int(sarr[3]),'month': int(sarr[4]), 'day': (sarr[2])}

        return pd.DataFrame(list(map(lambda v: transform(v), values))) \

    def create_pd_df(self, values: List[str]) -> pd.DataFrame:
        def transform(s: str):
            sarr = s.split('|')
            return {'repository_id': numpy.int64(sarr[0]), 'dist_commits': numpy.int64(sarr[1]),
                    'week_of_year': int(sarr[3]),'month': int(sarr[4]), 'day':  numpy.datetime64((sarr[2]))}

        return pd.DataFrame(list(map(lambda v: transform(v), values))) \


    def create_df(self, values: List[str]):
        def transform(s: str):
            sarr = s.split('|')
            return {'repository_id': int(sarr[0]), 'dist_commits': int(sarr[1]),  'week_of_year': int(sarr[3]),
                    'month': int(sarr[4]),'day': datetime.strptime(sarr[2],"%Y-%m-%d")}

        return self.spark.createDataFrame(list(map(lambda v: transform(v), values)))

    @staticmethod
    def assert_frame_equal_with_sort(results: DataFrame, expected: pd.DataFrame, key_columns):
        results_sorted = results.toPandas().sort_values(by=key_columns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=key_columns).reset_index(drop=True)
        assert_frame_equal(results_sorted, expected_sorted, check_dtype=False)

    def create_ggi_insights(self, csv='ggi_files_to_process.csv') -> GGiGitHubInsights:
        return GGiGitHubInsights(self.spark, f'{self.base_path}/{csv}.csv')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
