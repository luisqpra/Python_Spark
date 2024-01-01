# Transformaciones - funciones select y selectExpr
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# select

df = spark.read.parquet('./data/datos.parquet')

df.printSchema()

df.select(col('video_id')).show()

df.select('video_id', 'trending_date').show()

# Esta vía nos dará error

df.select(
    'likes',
    'dislikes',
    ('likes' - 'dislikes')
).show()

# Forma correcta

df.select(
    col('likes'),
    col('dislikes'),
    (col('likes') - col('dislikes')).alias('aceptacion')
).show()

# selectExpr

df.selectExpr('likes', 'dislikes', '(likes - dislikes) as aceptacion').show()

df.selectExpr("count(distinct(video_id)) as videos").show()
