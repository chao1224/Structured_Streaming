from pyspark.sql import SparkSession
from pyspark.sql.types import *

import sys

def display(x):
    print(x)

if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     exit(-1)

    # input_directory = args[1]
    # print(input_directory)
    # output_file = args[2]
    # print(output_file)

    spark = SparkSession.builder.appName('Count Mention Times within Last 10 seconds').getOrCreate()

    schema = StructType([StructField('A', StringType(), True),
                         StructField('B', StringType(), True),
                         StructField('ts', StringType(), True),
                         StructField('interaction', StringType(), True)])

    lines = spark.readStream.csv('/tweets/*.csv', schema)

    result = lines.select("B").where("interaction='MT'").collect()

    query1 = result.writeStream.format('console').trigger(processingTime='10 seconds').start()
    
    # query = result.writeStream\
    #     .trigger(processingTime='10 seconds')\
    #     .option("checkpointLocation",'/checkpoint')\
    #     .start(format='parquet',path='/out')

    # query2 = result.writeStream\
    #     .format('parquet')\
    #     .option("checkpointLocation",'/checkpoint')\
    #     .option("charset", "UTF-16")\
    #     .trigger(processingTime='10 seconds')\
    #     .start(path='/out')

    query1.awaitTermination()
    # query2.awaitTermination()