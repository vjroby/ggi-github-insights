### GGi - GGi GitHub Insights

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

Run tests

```shell script
python3 -m unittest
```


Create environment variables for spark:

```shell script
export SPARK_HOME=<path to spark 2.4.7>
```

## Improvements

1. Create and manage resource with terraform
2. separate repo for composer and dataproc
3. Get resources names automatically
4. Added a build test step in a docker
5. Use git hub tags for artifacts