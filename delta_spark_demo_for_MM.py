import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.tables import *

# python 3.13
# java -version
# openjdk version "1.8.0_242"
# OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_242-b08)
# OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.242-b08, mixed mode)

# If using local jars, you can use spark.jars instead for comma-delimited jar files. 
# For cluster setups, of course, you’ll have to put the jars in a reachable location for all driver and executor nodes.
# download jar from https://mvnrepository.com/artifact/io.delta/delta-spark_2.13/3.3.0 

builder = SparkSession.builder \
    .appName("DeltaSpark") \
    .master("local[*]") \
    .config("spark.driver.memory", "8G") \
    .config("spark.jars", "/path_to_jar/delta-sharing-spark_2.12-3.3.0.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(spark)

data = spark.range(0, 5)
data.show()
data.write.format("delta").save("/path_to_data/delta-table")

read_data = spark.read.format("delta").load("/path_to_data/delta-table")
read_data.show()


# conditional update
deltaTable = DeltaTable.forPath(spark, "/path_to_data/delta-table")
deltaTable.update(condition = expr("id %2 == 0"),
                  set = {"id": expr("id + 100")})

deltaTable.toDF().show()

