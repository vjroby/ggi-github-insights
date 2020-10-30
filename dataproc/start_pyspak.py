from pyspark.sql import SparkSession
from dataproc.github_insights_pyspark import GGiGitHubInsights
import logging

import argparse

log = logging.getLogger('start_pyspark')

parser = argparse.ArgumentParser(description='PySpark start script for GGi GitHub insights')
parser.add_argument('csv_filenames_path', metavar='C', required=True)
args = parser.parse_args()

log.info('Arguments parsed. Starting PySpark...')

spark = SparkSession.builder.getOrCreate()
log.info('SparkSession created. Initializing data processing')

ggi = GGiGitHubInsights(spark, args.csv_filenames_path)

