# Funciones min y max
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data')

vuelos.printSchema()


vuelos.select(
    min('AIR_TIME').alias('menor_timepo'),
    max('AIR_TIME').alias('mayor_tiempo')
).show()

vuelos.select(
    min('AIRLINE_DELAY'),
    max('AIRLINE_DELAY')
).show()
