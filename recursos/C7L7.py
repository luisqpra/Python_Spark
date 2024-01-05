# Agregación con pivote
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg

spark = SparkSession.builder.getOrCreate()

estudiantes = spark.read.parquet('./data/')

estudiantes.show()

estudiantes.groupBy('graduacion').pivot('sexo').\
    agg(avg('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo').\
    agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['M']).\
    agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['F']).\
    agg(avg('peso'), min('peso'), max('peso')).show()
