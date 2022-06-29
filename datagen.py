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
    dataspec = (dg.DataGenerator(spark, rows=row_count, randomSeed=random.randint(0, row_count)))#.withSchema(table_schema))
    dataspec = (dataspec
#                     .withColumnSpec("fn_periodo", values=[0])
#                     .withColumnSpec("cod_entidad_or", uniqueValues=276, random=True)  
#                     .withColumnSpec("cod_operacion", uniqueValues=51356072, random=True)
#                     .withColumnSpec("cod_persona", uniqueValues=21942951, random=True)
#                     .withColumnSpec("cod_naturaleza_intervencion", uniqueValues=15, random=True, format='T%02d')
#                     .withColumnSpec("cod_situacion_relacion", uniqueValues=2, random=True)
#                     .withColumnSpec("cod_convenio_acree", uniqueValues=3, random=True)                           
#                     .withColumnSpec("filler", values=['...'])
                    .withColumn("id", IntegerType())
                    .withColumn("fn_periodo", IntegerType(), values=[0])
                    .withColumn("cod_entidad_or", IntegerType(), uniqueValues=276, random=True)  
                    .withColumn("cod_operacion", IntegerType(), uniqueValues=51356072, random=True)
                    .withColumn("cod_persona", IntegerType(), uniqueValues=21942951, random=True)
                    .withColumn("cod_naturaleza_intervencion", IntegerType(), uniqueValues=15, random=True, format='T%02d')
                    .withColumn("cod_situacion_relacion", IntegerType(), uniqueValues=2, random=True)
                    .withColumn("cod_convenio_acree", IntegerType(), uniqueValues=3, random=True)                           
                    .withColumn("filler", StringType(), values=['...'])
                )
    df = dataspec.build()
    df.write.format(format_).mode('overwrite').save(path)
    return df
