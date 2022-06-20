# Spark-RAPIDS

Ciertas operaciones en Spark pueden acelerarse haciendo uso de GPUs gracias a la librería [RAPIDS](https://nvidia.github.io/spark-rapids/) proporcionada por NVIDIA.


### Instalación de Spark, RAPIDS y CUDA
- Ejecutar [`install-spark.sh`](https://github.com/pablodll/Spark-RAPIDS/blob/main/install-spark.sh) para descargar y descomprimir _Spark 3.2.1_ y descargar las librerías _rapids_ y _cudf_ (tambien instala la libreria de python _dbldatagen_ para poder generar datos sintéticos con los que trabajar)

### Ejecutar Spark (PySpark)
- PySpark en CPU: ejecutar [`spark-cpu.sh`](https://github.com/pablodll/Spark-RAPIDS/blob/main/spark-cpu.sh)
- PySpark en GPU: ejecutar [`spark-gpu.sh`](https://github.com/pablodll/Spark-RAPIDS/blob/main/spark-gpu.sh)
- Para cargar los ficheros de python y usarlos en spark: `exec(open("file").read())`

### Generación de datos sintéticos
- Ejecutar desde PySpark el método `datagen()` de [`datagen.py`](https://github.com/pablodll/Spark-RAPIDS/blob/main/datagen.py) especificando el número de líneas a generar, para obtener un dataframe de datos sintéticos siguiendo el esquema especificado en [`schema.json`](https://github.com/pablodll/Spark-RAPIDS/blob/main/schema.json) y escribirlo en disco usando el formato _csv_. 
  > Ejemplo: 
  > ```python
  > from datagen import datagen
  > df = datagen(row_count = 1000) #Generar dataframe de 1000 líneas
  > ``` 

### Pruebas con medición de tiempo
- En [```time_test.py```](https://github.com/pablodll/Spark-RAPIDS/blob/main/time_test.py) se implementa una clase que proporciona varios métodos básicos para tratar con dataframes de Spark (_read, show, count, select_...) que miden el tiempo de ejecución de cada prueba. Además, existe la opción de guardar los resultados en un fichero de log.
  - En este fichero se proporciona el método ```run_tests()``` que recibe una lista de nombres de dataframes o busca todos los dataframes existentes con nombre _DF\_*_ y ejecuta una serie de pruebas básicas, mostrando el tiempo empleado en cada una y escribiéndolo en el fichero de log.
  > Ejemplo:
  >```python
  > t = time_test("log.txt")
  > t.run_tests()
  >```
