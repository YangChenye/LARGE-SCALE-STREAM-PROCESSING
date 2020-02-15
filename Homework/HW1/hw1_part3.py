# Created by Chenye Yang on 2020/2/12.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *
import re
import csv

# Make a regular expression for validating an Ip-address
regex = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'

conf = pyspark.SparkConf().setAppName('hw1_part3_ChenyeYang').setMaster('local[*]') # set the configuration
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('epa-http.txt') # read file line by line to creat RDDs
# use ' ' to split the string
# filter the RDD with valid ip address and valid http return code
# 302 is not a successful return code, thus '-' should be discarded
log_lines_validip = log_lines.filter(lambda x: re.search(regex, x.split(' ')[0]) and x.split(' ')[-1] != '-')
# transform RDDs into pair RDDs ('ip', datetime, bytes).
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string
# use "datetime()" to change the time string to real time
# 141.243.1.172 [29:23:53:25] "GET /Software.html HTTP/1.0" 200 1497
log_pairs = log_lines_validip.map(lambda x:
                          (x.split(' ')[0],
                           datetime.strptime('20-01-{}{}'.format(x.split(' ')[1][1:3], x.split(' ')[1][4:-1]), '%y-%m-%d%H:%M:%S'),
                           int(x.split(' ')[-1])))

# use spark sql to ensure the deal with log data in even time, rather than received time, i.e. deal with the late data
# creat a spark session
spark = SparkSession \
    .builder \
    .appName('hw1_part3_ChenyeYang') \
    .getOrCreate()

# set the schema of data frame
schema = StructType([
    StructField('ip', StringType(), True),
    StructField('datetime', TimestampType(), True),
    StructField('bytes', IntegerType(), True)])

# creat data frame with RDDs and schema
# columns in log_dataframe are ['ip', 'datetime', 'bytes']
log_dataframe = spark.createDataFrame(log_pairs, schema)

# with watermark, we can handle the late data properly. Discard very late data and keep not very late data.
# with window size = 60min slide = 60min, we group the log in one hour by "datetime"
# with groupBy, we groups the DataFrame using the specified columns, so we can run aggregation on them.
# with agg, we aggregate the items in window and "ip", the result should be the sum of "bytes"
# with orderBy, we get new DataFrame sorted by the specified column(s) by timestamp ascending and total bytes descending
# with show, we print 20 sorted items without truncating strings longer than 20 chars
# CONTENT IN log_windowed:
# columns in log_windowed are ['window', 'ip', 'sum(bytes)']
# data types are [('window', 'struct<start:timestamp,end:timestamp>'), ('ip', 'string'), ('sum(bytes)', 'bigint')]
log_windowed = log_dataframe \
    .withWatermark('datetime', '10 minutes') \
    .groupBy(window(log_dataframe.datetime, '60 minutes', '60 minutes'), log_dataframe.ip) \
    .agg(sum('bytes')) \
    .orderBy(['window.start', 'sum(bytes)'], ascending=[True, False])
log_windowed.show(20, truncate=False)

# window.start and window.end are timestamps and we can use the built in method to get year(), month(), day()
# use filter() and SQL like commands to select the specific data in one day between some hours
log_specific_time_interval = log_windowed \
    .filter('year(window.start) == 2020') \
    .filter('month(window.start) == 01') \
    .filter('day(window.start) == 30') \
    .filter('Hour(window.start) == 00 AND Hour(window.end) == 01')

log_specific_time_interval.show(20, truncate=False)

# drop the 'window' column because it is not in output csv file
# sort by ip in string ascending order so 100.1.2.3 comes before 99.2.3.5.
log_csv = log_specific_time_interval.drop('window').sort('ip', ascending=True)

# save RDD as CSV
f = open('hw1_part3.csv', 'w', newline='')
f_csv = csv.writer(f)
f_csv.writerows(log_csv.collect())
