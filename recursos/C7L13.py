# Full Outer Join
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

empleados.join(departamentos, col('num_dpto') == col('id'), 'outer').show()
