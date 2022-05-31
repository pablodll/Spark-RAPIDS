apt-get install openjdk-8-jdk-headless -qq > /dev/null

wget -q https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
wget -q https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar
wget -q https://repo1.maven.org/maven2/ai/rapids/cudf/22.04.0/cudf-22.04.0.jar

tar xf spark-3.2.1-bin-hadoop3.2.tgz

!pip install git+https://github.com/databrickslabs/dbldatagen
