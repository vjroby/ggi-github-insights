import unittest

import os

os.environ['OUTPUT_BUCKET'] = ""
os.environ['PYSPARK_BUCKET'] = ""
os.environ['CLUSTER_NAME'] = ""
os.environ['PYSPARK_MAIN_PATH'] = ""
os.environ['PYSPARK_ARCHIVE_PATH'] = ""

from composer.github_insights_flow import create_increment_dates

class TestStringMethods(unittest.TestCase):

    def test_create_increment_dates(self):
        # the dates are inclusive so here there are 32 days * 24 hours
        start_date = "2014-03-01"
        end_date = "2014-04-01"
        result = create_increment_dates(start_date, end_date)
        self.assertEqual(len(result), 768)
        self.assertEqual(result[0], "https://data.gharchive.org/2014-03-01-0.json.gz")
        self.assertEqual(result[-1], "https://data.gharchive.org/2014-04-01-23.json.gz")

    def test_create_increment_dates_format(self):
        # the dates are inclusive so here there are 32 days * 24 hours
        start_date = "2014-03-01"
        end_date = "2014-03-03"
        result = create_increment_dates(start_date, end_date)
        self.assertEqual(result[0], "https://data.gharchive.org/2014-03-01-0.json.gz")
        self.assertEqual(result[24], "https://data.gharchive.org/2014-03-02-0.json.gz")
        self.assertEqual(result[-1], "https://data.gharchive.org/2014-03-03-23.json.gz")


if __name__ == "__main__":
    unittest.main()
