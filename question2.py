from pyspark.sql import SparkSession
from pyspark.sql.types import *

import sys


def printToConsole():
    spark = SparkSession.builder.appName('Count Mention Times within Last 10 seconds').getOrCreate()

    schema = StructType([StructField('A', StringType(), True),
                         StructField('B', StringType(), True),
                         StructField('ts', StringType(), True),
                         StructField('interaction', StringType(), True)])

    lines = spark.readStream.csv(input_directory+'/*.csv', schema)

    result = lines.select("B").where("interaction='MT'")

    query = result.writeStream.format('console').trigger(processingTime='10 seconds').start()

    query.awaitTermination()

def printToHDFS():
    spark = SparkSession.builder.appName('Count Mention Times within Last 10 seconds').getOrCreate()

    schema = StructType([StructField('A', StringType(), True),
                         StructField('B', StringType(), True),
                         StructField('ts', StringType(), True),
                         StructField('interaction', StringType(), True)])

    lines = spark.readStream.csv(input_directory+'/*.csv', schema)

    result = lines.select("B").where("interaction='MT'")

    query = result.writeStream \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", '/checkpoint1') \
        .start(format='parquet', path=output_directory)

    # query2 = result.writeStream\
    #     .format('parquet')\
    #     .option("checkpointLocation",'/checkpoint')\
    #     .option("charset", "UTF-16")\
    #     .trigger(processingTime='10 seconds')\
    #     .start(path='/out')

    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        exit(-1)
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    spark = SparkSession.builder.appName('Count Mention Times within Last 10 seconds').getOrCreate()
    schema = StructType([StructField('A', StringType(), True),
                         StructField('B', StringType(), True),
                         StructField('ts', StringType(), True),
                         StructField('interaction', StringType(), True)])
    lines = spark.readStream.csv(input_directory+'/*.csv', schema)
    result = lines.select("B").where("interaction='MT'")
    query = result.writeStream\
        .format('parquet')\
        .option('path',output_directory)\
        .option("checkpointLocation",'/checkpoint1')\
        .trigger(processingTime='10 seconds') \
        .start()
    query.awaitTermination()