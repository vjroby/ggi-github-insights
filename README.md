### GGi - GGi GitHub Insights

The latest version of python that works with apache spark 2.3.4 is python 3.7
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
python -m unittest
```


Create environment variables for spark:

```shell script
export SPARK_HOME=<path to spark 2.3.4>
```