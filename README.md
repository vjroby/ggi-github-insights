### GGi - GGi GitHub Insights

Project Structure per folder

- `build-pipeline` - script used when building the project
- `composer` - GCP airflow code
- `dataproc` - GCP pyspark code
- `notebooks` - jupyter notebook code for data exploration

Apache spark is bound to dataproc vm version (1.5)[https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.5]


The latest version of python that works with apache spark 2.4.5 is python 3.7
Create environment (gienv):

```shell script
python -m pip install virtualenv
python -m virtualenv --python=python3.7 gienv
```
Activate the environment:

```shell script
source gienv/bin/activate
```

Download dependecies

```shell script
pip install -r requirements.txt
```

Run tests for both airflow and pyspark

```shell script
python3 -m unittest
```


Create environment variables for spark:

```shell script
export SPARK_HOME=<path to spark 2.4.7>
```

## Improvements

1. Create and manage resource with terraform
2. Get resources names automatically in the build step
3. Added a build test step in a docker container
4. Use git hub tags for artifacts
5. Create a last build step to check that the DAG is running
6. Use VPC with FW rules to better