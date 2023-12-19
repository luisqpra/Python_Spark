# SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder.master(
    "local[*]").appName('Curso Pyspark').getOrCreate()

spark.stop()
