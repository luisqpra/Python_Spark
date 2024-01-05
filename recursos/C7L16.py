# Cross Join
from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

df = empleados.crossJoin(departamentos)

df.show()

df.count()
