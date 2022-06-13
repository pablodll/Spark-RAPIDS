apt-get install openjdk-8-jdk-headless -qq > /dev/null

wget -q https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
wget -q https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar
wget -q https://repo1.maven.org/maven2/ai/rapids/cudf/22.04.0/cudf-22.04.0.jar

tar xf spark-3.2.1-bin-hadoop3.2.tgz

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/cuda-ubuntu1804.pin
sudo mv cuda-ubuntu1804.pin /etc/apt/preferences.d/cuda-repository-pin-600
wget https://developer.download.nvidia.com/compute/cuda/11.0.3/local_installers/cuda-repo-ubuntu1804-11-0-local_11.0.3-450.51.06-1_amd64.deb
sudo dpkg -i cuda-repo-ubuntu1804-11-0-local_11.0.3-450.51.06-1_amd64.deb
sudo apt-key add /var/cuda-repo-ubuntu1804-11-0-local/7fa2af80.pub
sudo apt-get update
sudo apt-get -y install cuda

pip install git+https://github.com/databrickslabs/dbldatagen
pip install numpy
pip install pandas
