# Funciones de ventana

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, rank, dense_rank, col
from pyspark.sql.functions import min, max, avg

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df.show()

windowSpec = Window.partitionBy('departamento').orderBy(desc('evaluacion'))

# row_number

df.withColumn('row_number', row_number().over(windowSpec))\
    .filter(col('row_number').isin(1, 2)).show()

# rank

df.withColumn('rank', rank().over(windowSpec)).show()

# dense_rank

df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()

# Agregaciones con especificaciones de ventana

windowSpecAgg = Window.partitionBy('departamento')

(df.withColumn('min', min(col('evaluacion')).over(windowSpecAgg))
    .withColumn('max', max(col('evaluacion')).over(windowSpecAgg))
    .withColumn('avg', avg(col('evaluacion')).over(windowSpecAgg))
    .withColumn('row_number', row_number().over(windowSpec))
 ).show()
