from pyspark.sql import SparkSession
from dataproc.github_insights_pyspark import GGiGitHubInsights

import argparse

parser = argparse.ArgumentParser(description='PySpark start script for GGi GitHub insights')
parser.add_argument('bucket', metavar='B')
parser.add_argument('csv_filename', metavar='C')
args = parser.parse_args()

print(f"Arguments parsed. Bucket:{args.bucket}  csv_filename:{args.csv_filename}  "
      f"Starting PySpark...")

spark = SparkSession.builder\
      .appName('ggi-spark') \
      .getOrCreate()

print('SparkSession created. Initializing data processing')
csv_filenames_path = f"{args.bucket}/{args.csv_filename}"

ggi = GGiGitHubInsights(spark, csv_filenames_path)

print('Read csv to get file names')
filenames_df = ggi.get_json_filenames()
filenames_df.show()

print('Convert to list')
files = ggi.convert_to_list(filenames_df)

print('Read all files')
files_df = ggi.read_files(files, args.bucket)

print('Union all files')
files_unioned = ggi.union_dfs(files_df)

print("Grouped by day, month and year")

grouped = ggi.group_by_columns(files_unioned, ['day', 'month', 'week_of_year'])

grouped['day'].show()
grouped['month'].show()
grouped['week_of_year'].show()

print("Saving to BigQuery...")
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.

spark.conf.set('temporaryGcsBucket', "data_gharchive_org_20203010")

grouped['day'].write.format('bigquery')\
      .mode('append') \
      .option('table','ggi_insights.commits_by_day')\
      .save()

grouped['month'].write.format('bigquery')\
      .mode('append') \
      .option('table','ggi_insights.commits_by_month')\
      .save()

grouped['week_of_year'].write.format('bigquery')\
      .mode('append') \
      .option('table','ggi_insights.commits_by_week_of_year')\
      .save()


