# Agregación con agrupación
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data/')

vuelos.printSchema()

vuelos.groupBy('ORIGIN_AIRPORT')\
    .count()\
    .orderBy(desc('count')).show()

vuelos.groupBy('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT')\
    .count()\
    .orderBy(desc('count'))\
    .show()
