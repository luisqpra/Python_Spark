# Funciones sum, sumDistinct y avg

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, sumDistinct, avg, count

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data/')

# sum
vuelos.printSchema()

vuelos.select(
    sum('DISTANCE').alias('sum_dis')
).show()

# sumDistinct

vuelos.select(
    sumDistinct('DISTANCE').alias('sum_dis_dif')
).show()

# avg

vuelos.select(
    avg('AIR_TIME').alias('promedio_aire'),
    (sum('AIR_TIME') / count('AIR_TIME')).alias('prom_manual')
).show()
