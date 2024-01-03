# Trabajo con datos incorrectos o faltantes

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df.count()

df.na.drop().count()

df.na.drop('any').count()

df.dropna().count()

df.na.drop(subset=['views']).count()

df.na.drop(subset=['views', 'dislikes']).count()

df.orderBy(col('views')).select(col('views'),
                                col('likes'),
                                col('dislikes')).show()

df.fillna(0).orderBy(col('views')).select(col('views'),
                                          col('likes'),
                                          col('dislikes')).show()

df.fillna(0, subset=['likes', 'dislikes']).orderBy(col('views')).select(
    col('views'), col('likes'), col('dislikes')).show()
