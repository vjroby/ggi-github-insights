 #!/bin/bash
cd ..
rm -f ./package.zip
zip -r ./package.zip ./dataproc/ -x *test* *__pycache__* start_pyspak.py
