 #!/bin/bash
rm -f ../package.zip
zip -r ../package.zip ../dataproc/github_insights_pyspark.py ../dataproc/start_pyspak.py   -x run.py \*.pyc \*__pycache__* \*.sh
cd ..