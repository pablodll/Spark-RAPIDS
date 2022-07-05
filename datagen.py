import dbldatagen as dg
import random
import json
from pyspark.sql.types import FloatType, IntegerType, StringType, StructType

def load_schema(file):
    with open(file, 'r') as content_file:
        schema_json = content_file.read()
        
    return StructType.fromJson(json.loads(schema_json))
    

def datagen(row_count, path="DATAGEN_DF", schema='schema.json', format_='csv'):
    table_schema = load_schema(schema)
    dataspec = (dg.DataGenerator(spark, rows=row_count, randomSeed=random.randint(0, row_count)).withSchema(table_schema))
    dataspec = (dataspec
#                     .withIdOutput()
                    .withColumnSpec("fn_periodo", values=[0])
                    .withColumnSpec("cod_entidad_or", uniqueValues=276, random=True)  
                    .withColumnSpec("cod_operacion", uniqueValues=51356072, random=True)
                    .withColumnSpec("cod_persona", uniqueValues=21942951, random=True)
                    .withColumnSpec("cod_naturaleza_intervencion", uniqueValues=15, random=True, format='T%02d')
                    .withColumnSpec("cod_situacion_relacion", uniqueValues=2, random=True)
                    .withColumnSpec("cod_convenio_acree", uniqueValues=3, random=True)                           
                    .withColumnSpec("filler", values=['...'])
                )
    df = dataspec.build()
    df.write.format(format_).option('header', True).mode('overwrite').save(path)
#     df.write.mode('overwrite').csv(path, header=True)
    return df

def aux_datagen(column, name="cod_entidad_or", path="AUX_DF", format_='csv'):
    column_ls = column.distinct().rdd.flatMap(lambda x : x).collect()
    dataspec = (dg.DataGenerator(spark, rows=len(column_ls), randomSeed=random.randint(0, len(column_ls))))
    dataspec = (dataspec
                    .withColumn('id_'+name, IntegerType(), minValue=1, maxValue=len(column_ls), step=1, format='ID%04d')
                    .withColumn(name, IntegerType(), values=column_ls)
                )
    df = dataspec.build()
    df.write.format(format_).option('header', True).mode('overwrite').save(path)
    return df
