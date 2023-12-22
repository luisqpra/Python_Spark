# Acciones: función count
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(['j', 'o', 's', 'e'])

rdd.count()

rdd1 = sc.parallelize([item for item in range(10)])

rdd1.count()
