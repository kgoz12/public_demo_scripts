import pyspark
from pyspark.sql.functions import udf, explode
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row

import pandas as pd
import math
import requests
from requests.adapters import HTTPAdapter

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.memory", "8G") \
    .getOrCreate()

print(spark)    

# https://restful-api.dev
# https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations

# this is a toy dataframe that contains the data elements required for the POST example
df= spark.createDataFrame([
    Row(name="Apple MacBook Pro 16", year=2019, price=1849.99, CPU_model="Intel Core i9", Hard_disk_size="1 TB")
])
df.show()

# now let's take the flat table (similar to what we have in our data store)
# and let's convert that to a json string request with the required nesting structure and field names
df_json_request = df\
    .withColumnRenamed("CPU_model", "CPU model")\
    .withColumnRenamed("Hard_disk_size", "Hard disk size")\
    .withColumn("data", F.struct("year", "price", "CPU model", "Hard disk size"))\
    .withColumn("x", F.to_json(F.struct("name", "data")))\
    .select("x")
df_json_request.show(truncate=False)
df_json_request.printSchema()

# this is the endpoint of the API
restapi_uri = "https://api.restful-api.dev/objects"

# with pretty print, this is what the json would look like...
# x = """
# {
#    "name": "Apple MacBook Pro 16",
#    "data": {
#       "year": 2019,
#       "price": 1849.99,
#       "CPU model": "Intel Core i9",
#       "Hard disk size": "1 TB"
#    }
# }
# """

### UDF for making a rowwise call to the REST (POST) API. (session.get for GET requests)
### T.IntegerType() = key type (should be 200 when request successful)
### T.BinaryType() = value type (b/c api is passing result back as byte string)
### True means result can be null
@udf(T.MapType(T.IntegerType(), T.BinaryType(), True))
def callRestApiOnce(x):
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=3)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    headers = {'content-type': 'application/json'}
    response = session.post(restapi_uri, headers=headers, data=x, verify=False)
    if not (response.status_code==200 or response.status_code==201):
        raise Exception("Response status : {} .Response message : {} ".\
                        format(str(response.status_code), response.text))
    
    return {response.status_code : response.content}


### this is the schema of the expected json response
response_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("createdAt", T.TimestampType(), True),
    T.StructField("data", T.StructType([
        T.StructField("year", T.IntegerType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("CPU model", T.StringType(), True),
        T.StructField("Hard disk size", T.StringType(), True)
    ]), True)
])

### this returns the result of the API
### when the key in the map is 200, the value is the response content
### if the key is something like 400, value is an error message
df_api_result = df_json_request.withColumn("api_response", callRestApiOnce(F.col("x")))\
    .withColumn("api_response_code", F.map_keys("api_response"))\
    .withColumn("api_response_content", F.map_values("api_response"))\
    .withColumn("api_response_content_exploded", F.explode_outer("api_response_content"))\
    .withColumn("api_response_content_text", F.decode("api_response_content_exploded", 'UTF-8'))\
    .withColumn("api_response_structured", F.from_json("api_response_content_text", schema=response_schema))\
    .drop("x", "api_response", "api_response_content", "api_response_content_exploded", "api_response_content_text")
df_api_result.show(truncate = False)
df_api_result.printSchema()

### TODO: now we need to think about scalable implementation on ~11.5M records monthly