# Created by Chenye Yang on 2020/2/12.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *

conf = pyspark.SparkConf().setAppName('hw1_part3_ChenyeYang').setMaster('local[*]') # set the configuration
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('epa-http.txt') # read file line by line to creat RDDs

# transform RDDs into pair RDDs ('ip', datetime, bytes).
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string, last value may be '-', which can not be transferred to int and should be counted as 0
# use datetime to change the time string to real time
# 141.243.1.172 [29:23:53:25] "GET /Software.html HTTP/1.0" 200 1497
log_pairs = log_lines.map(lambda x:
                          (x.split(' ')[0],
                           datetime.strptime('20-01-{}{}'.format(x.split(' ')[1][1:3], x.split(' ')[1][4:-1]), '%y-%m-%d%H:%M:%S'),
                           int(x.split(' ')[-1]) if x.split(' ')[-1] != '-' else 0))

# use spark sql to ensure the deal with log data in even time, rather than received time, i.e. deal with the late data
# creat a spark session
spark = SparkSession \
    .builder \
    .appName("hw1_part3_ChenyeYang") \
    .getOrCreate()

# set the schema of data frame
schema = StructType([
    StructField("ip", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("bytes", IntegerType(), True)])

# creat data frame with RDDs and schema
log_dataframe = spark.createDataFrame(log_pairs, schema)

# with watermark, we can handle the late data properly. Discard very late data and keep not very late data.
# with window size = 60min slide = 60min, we group the log in one hour by "datetime"
# with groupBy, we groups the DataFrame using the specified columns, so we can run aggregation on them.
# with agg, we aggregate the items in window and "ip", the result should be the sum of "bytes"
# with orderBy, we get a new DataFrame sorted by the specified column(s) by ascending or descending
# with show, we print 20 sorted items without truncating strings longer than 20 chars
log_windowed = log_dataframe \
    .withWatermark("datetime", "10 minutes") \
    .groupBy(window(log_dataframe.datetime, "60 minutes", "60 minutes"), log_dataframe.ip) \
    .agg(sum("bytes")) \
    .orderBy(["window.start", "sum(bytes)"], ascending=[True, False]) \
    .show(20, truncate=False)

