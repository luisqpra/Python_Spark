# SparkSession

import findspark
from pyspark.sql import SparkSession

findspark.init()

spark = SparkSession.builder.master(
    "local[*]").appName('Curso Pyspark').getOrCreate()

spark.stop()
