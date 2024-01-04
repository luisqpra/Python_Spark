# Funciones count, countDistinct y approx_count_distinct

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import approx_count_distinct

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/dataframe')

df.printSchema()

df.show()

# count
df.select(
    count('nombre').alias('conteo_nombre'),
    count('color').alias('conteo_color')
).show()

df.select(
    count('nombre').alias('conteo_nombre'),
    count('color').alias('conteo_color'),
    count('*').alias('conteo_general')
).show()

# countDistinct
df.select(
    countDistinct('color').alias('colores_dif')
).show()

# approx_count_distinct

dataframe = spark.read.parquet('./data/vuelos')

dataframe.printSchema()

dataframe.select(
    countDistinct('AIRLINE'),
    approx_count_distinct('AIRLINE')
).show()
