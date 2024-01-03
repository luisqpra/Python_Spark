# Acciones sobre un dataframe en Spark SQL
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

# show

df.show()

df.show(5)

df.show(5, truncate=False)

# take

df.take(1)

# head

df.head(1)

# collect

df.select('likes').collect()
