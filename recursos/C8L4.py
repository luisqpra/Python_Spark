# Funciones when, coalesce y lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/')

data.show()

data.select(
    col('nombre'),
    when(col('pago') == 1, 'pagado')
    .when(col('pago') == 2, 'sin pagar')
    .otherwise('sin iniciar').alias('pago')
).show()

data.select(
    coalesce(col('nombre'), lit('sin nombre')).alias('nombre')
).show()
