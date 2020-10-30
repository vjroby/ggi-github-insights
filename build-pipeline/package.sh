 #!/bin/bash
rm -f ../package.zip
zip -r ../package.zip ../dataproc/github_insights_pyspark.py ../dataproc/start_pyspak.py ../dataproc/__init__.py  -x \*.pyc \*__pycache__*
cd ..