# Trabajo con columnas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/dataPARQUET.parquet')

df.printSchema()

# Primera alternativa para referirnos a las columnas

df.select('title').show()

# Segunda alternativa

df.select(col('title')).show()
