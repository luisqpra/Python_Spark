# Transformaciones: función repartition

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1, 2, 3, 4, 5], 3)

rdd.getNumPartitions()

rdd7 = rdd.repartition(7)

rdd7.getNumPartitions()
