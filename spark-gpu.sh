./spark-3.2.1-bin-hadoop3.2/bin/pyspark --num-executors=1 --conf spark.plugins=com.nvidia.spark.SQLPlugin --jars '/Spark-RAPIDS/rapids-4-spark_2.12-22.04.0.jar,/Spark-RAPIDS/cudf-22.04.0-cuda11.jar'
