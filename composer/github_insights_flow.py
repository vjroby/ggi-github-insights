from datetime import datetime, timedelta, date
from typing import Tuple, List

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
import os
import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

OUTPUT_BUCKET = "data_gharchive_org_2020-01_02_us"
PYSPARK_BUCKET = "data_gharchive_org_2020-01_02_us"
CLUSTER_NAME = "cluster-aa02"
PYSPARK_MAIN_FILENAME = "github_insights_pyspark.py"
date_tag = date.today().strftime('%Y%m%d')
base_folder = '/home/airflow/gcs/data'
GGI_FILES_TO_PROCESS = "ggi_files_to_process.csv"
filenames_path = f"{base_folder}/{GGI_FILES_TO_PROCESS}"
gh_archive_start_date = "2014-03-01"
gh_archive_end_date = "2014-03-02"


def create_combined_tasks(download_link, dag, bucket) -> Tuple[BashOperator,str]:
    file_name = download_link.replace("https://data.gharchive.org/", "")
    download_task = BashOperator(
        task_id=f"download_{file_name}",
        bash_command=f"curl {download_link} | gsutil cp - gs://{bucket}/{file_name}",
        # bash_command=f"wget {download_link} -O {file_name}",
        dag=dag
    )
    # move_task = BashOperator(
    #     task_id=f"copy_to_bucket_{file_name}",
    #     bash_command=f"gsutil mv {file_name} gs://{bucket}",
    #     dag=dag
    # )
    # download_task >> move_task
    # return download_task, move_task
    return download_task, file_name


def create_increment_dates(start, end) -> List[str]:
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d') + timedelta(days=1) - timedelta(hours=1)
    result = []
    pattern = 'https://data.gharchive.org/%Y-%m-%d-%-H.json.gz'  # `-` in front of u is Linux only, sorry Windows
    while start_dt <= end_dt:
        result.append(start_dt.strftime(pattern))
        start_dt = start_dt + timedelta(hours=1)
    return result


def save_to_csv(filenames ,path):
    with open(f'{path}', mode='w') as files_to_process:
        filenames_writer = csv.writer(files_to_process)
        [filenames_writer.writerow(fn) for fn in filenames]


dag = DAG(
    'github_insights',
    default_args=default_args,
    description='Downloads github events, copies them to a bucket, runs a dataproc task that puts the results in BigQuery',
    schedule_interval=timedelta(days=1),
)

# links = create_increment_dates(gh_archive_start_date, gh_archive_end_date)
links = ['https://data.gharchive.org/2014-03-01-0.json.gz', 'https://data.gharchive.org/2014-03-01-1.json.gz']
map_to_tasks = lambda link: create_combined_tasks(link, dag, OUTPUT_BUCKET)
tuple_tasks_filenames = list(map(map_to_tasks, links))
dl_tasks = list(map(lambda t: t[0], tuple_tasks_filenames))
filenames = list(map(lambda t: t[1], tuple_tasks_filenames))

save_filenames_task = PythonOperator(
    task_id="create_filenames_csv",
    python_callable=save_to_csv,
    op_kwargs={'filenames' : filenames, 'path': filenames_path},
    dag=dag
)

copy_filenames_to_gs = FileToGoogleCloudStorageOperator(
    task_id=f"copy_{GGI_FILES_TO_PROCESS}_to_gs",
    src=filenames_path,
    dst=GGI_FILES_TO_PROCESS,
    bucket=OUTPUT_BUCKET,
    dag=dag
)

dataproc_task = DataProcPySparkOperator(
    task_id="pyspark",
    cluster_name=CLUSTER_NAME,
    main=f"gs://{PYSPARK_BUCKET}/{PYSPARK_MAIN_FILENAME}",
    dag=dag
)


dl_tasks >> save_filenames_task >> copy_filenames_to_gs #>> dataproc_task >> clear_bucket

# bq_dataset_name = 'airflow_bq_dataset_{{ ds_nodash }}'
# bq_githib_commits_table_id = bq_dataset_name + '.github_commits'
# output_file = 'gs://{gcs_bucket}/github_commits.csv'.format(
#     gcs_bucket=gsc_bucket)
# # Perform query of Airflow GitHub commits
# bq_airflow_commits_query = bigquery_operator.BigQueryOperator(
#     task_id='bq_airflow_commits_query',
#     bql="""      SELECT commit, subject, message
#       FROM [bigquery-public-data:github_repos.commits]
#       WHERE repo_name contains 'airflow'
#       """,
#     destination_dataset_table=bq_githib_commits_table_id)
# # Export query result to Cloud Storage
# export_commits_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
#     task_id='export_airflow_commits_to_gcs',
#     source_project_dataset_table=bq_githib_commits_table_id,
#     destination_cloud_storage_uris=[output_file],
#     export_format='CSV')
