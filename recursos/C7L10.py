# Inner Join

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados')

departamentos = spark.read.parquet('./data/departamentos')

empleados.show()

departamentos.show()

# Inner join

join_df = empleados.join(departamentos, col('num_dpto') == col('id'))

join_df.show()

join_df = empleados.join(departamentos, col('num_dpto') == col('id'), 'inner')

join_df.show()

join_df = empleados.join(departamentos).where(col('num_dpto') == col('id'))

join_df.show()
