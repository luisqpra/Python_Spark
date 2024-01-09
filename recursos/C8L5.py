# Funciones definidas por el usuario UDF
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()


def f_cubo(n):
    return n * n * n


spark.udf.register('cubo', f_cubo, LongType())

spark.range(1, 10).createOrReplaceTempView('df_temp')

spark.sql("SELECT id, cubo(id) AS cubo FROM df_temp").show()


def bienvenida(nombre):
    return ('Hola {}'.format(nombre))


bienvenida_udf = udf(lambda x: bienvenida(x), StringType())

df_nombre = spark.createDataFrame([('Jose',), ('Julia',)], ['nombre'])

df_nombre.show()

df_nombre.select(
    col('nombre'),
    bienvenida_udf(col('nombre')).alias('bie_nombre')
).show()


@udf(returnType=StringType())
def mayuscula(s):
    return s.upper()


df_nombre.select(
    col('nombre'),
    mayuscula(col('nombre')).alias('may_nombre')
).show()


def cubo_pandas(a: pd.Series) -> pd.Series:
    return a * a * a


cubo_udf = pandas_udf(cubo_pandas, returnType=LongType())

x = pd.Series([1, 2, 3])

print(cubo_pandas(x))

df = spark.range(5)

df.select(
    col('id'),
    cubo_udf(col('id')).alias('cubo_pandas')
).show()
