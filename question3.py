from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: question3.py <HDFS Path> <Userlist>", file=sys.stderr)

    path = sys.argv[1]
    userlistpath = sys.argv[2]

    spark = SparkSession.builder.appName("CS-838-Assignment2-PartB-Question3").getOrCreate()

    tweetSchema = StructType().add("userA", IntegerType()).add("userB", IntegerType()).add("timestamp", TimestampType()).add("interaction", StringType())
    userlist_schema = StructType().add("userA", IntegerType())
    userlist = spark.read.schema(userlist_schema).csv(userlistpath)
    userlist.show()
    csvDF = spark.readStream.schema(tweetSchema).csv(path)
    tweets = csvDF.groupBy('userA').count()
    # joined = tweets.join(userlist, 'userA', 'inner').select('userA', 'count')

    query = tweets.writeStream.outputMode('complete').format('console').start()
    query.awaitTermination()
