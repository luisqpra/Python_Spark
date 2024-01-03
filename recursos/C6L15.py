# Persistencia de DataFrames
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'valor'])

df.show()

df.persist()

df.unpersist()

df.cache()

df.persist(StorageLevel.DISK_ONLY)

df.persist(StorageLevel.MEMORY_AND_DISK)
