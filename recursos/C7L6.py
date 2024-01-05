# Varias agregaciones por grupo
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max, desc, avg

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data/')

vuelos.groupBy('ORIGIN_AIRPORT').agg(
    count('AIR_TIME').alias('tiempo_aire'),
    min('AIR_TIME').alias('min'),
    max('AIR_TIME').alias('max')
).orderBy(desc('tiempo_aire')).show()

vuelos.groupBy('MONTH').agg(
    count('ARRIVAL_DELAY').alias('conteo_de_retrasos'),
    avg('DISTANCE').alias('prom_dist')
).orderBy(desc('conteo_de_retrasos')).show()
