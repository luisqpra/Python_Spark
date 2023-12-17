# Instalar SDK Java 8

# !apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Descargar Spark 3.3.3

# !wget -q http://apache.osuosl.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz

# Descomprimir el archivo descargado de Spark

# !tar xf spark-3.3.3-bin-hadoop3.tgz

# Establecer las variables de entorno
import findspark
from pyspark.sql import SparkSession
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.3-bin-hadoop3"

# Instalar la librería findspark 

# !pip install -q findspark

# Instalar pyspark

# !pip install -q pyspark

# verificar la instalación 

findspark.init()

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Probando la sesión de Spark
df = spark.createDataFrame([{"Hola": "Mundo"} for x in range(10)])

df.show(10, False)
