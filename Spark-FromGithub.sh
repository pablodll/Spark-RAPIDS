#!/bin/sh

git clone https://github.com/pablodll/Spark-RAPIDS.git
echo Git repo cloned

cd /Spark-RAPIDS
chmod 777 /Spark-RAPIDS/install-spark.sh
chmod 777 /Spark-RAPIDS/spark-cpu.sh
chmod 777 /Spark-RAPIDS/spark-gpu.sh

echo Installing Spark...
/Spark-RAPIDS/install-spark.sh
