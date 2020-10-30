 #!/bin/bash
cd ..
rm -f ./package.zip
zip -r ./package.zip ./dataproc/ -x *testdata* *__pycache__*
