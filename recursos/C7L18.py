# Shuffle Hash Join y Broadcast Hash Join
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

empleados.join(broadcast(
    departamentos), col('num_dpto') == col('id')).show()

empleados.join(broadcast(
    departamentos), col('num_dpto') == col('id')).explain()
