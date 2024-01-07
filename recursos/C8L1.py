# Funciones de fecha y hora
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.functions import date_format
from pyspark.sql.functions import datediff, months_between, last_day
from pyspark.sql.functions import date_add, date_sub
from pyspark.sql.functions import (year, month, dayofmonth,
                                   dayofyear, hour, minute, second)

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/convertir')

data.printSchema()

data.show(truncate=False)

data1 = data.select(
    to_date(col('date')).alias('date1'),
    to_timestamp(col('timestamp')).alias('ts1'),
    to_date(col('date_str'), 'dd-MM-yyyy').alias('date2'),
    to_timestamp(col('ts_str'), 'dd-MM-yyyy mm:ss').alias('ts2')

)

data1.show(truncate=False)

data1.printSchema()

data1.select(
    date_format(col('date1'), 'dd-MM-yyyy')
).show()

df = spark.read.parquet('./data/calculo')

df.show()

df.select(
    col('nombre'),
    datediff(col('fecha_salida'), col('fecha_ingreso')).alias('dias'),
    months_between(col('fecha_salida'), col('fecha_ingreso')).alias('meses'),
    last_day(col('fecha_salida')).alias('ultimo_dia_mes')
).show()

df.select(
    col('nombre'),
    col('fecha_ingreso'),
    date_add(col('fecha_ingreso'), 14).alias('mas_14_dias'),
    date_sub(col('fecha_ingreso'), 1).alias('menos_1_dia')
).show()

df.select(
    col('baja_sistema'),
    year(col('baja_sistema')),
    month(col('baja_sistema')),
    dayofmonth(col('baja_sistema')),
    dayofyear(col('baja_sistema')),
    hour(col('baja_sistema')),
    minute(col('baja_sistema')),
    second(col('baja_sistema'))
).show()
