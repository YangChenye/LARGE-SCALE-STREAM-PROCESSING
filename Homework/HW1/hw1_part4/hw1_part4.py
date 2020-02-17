# Created by Chenye Yang on 2020/2/15.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re
import csv

# Make a regular expression for validating an Ip-address
regex = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'

conf = pyspark.SparkConf().setAppName('hw1_part4_ChenyeYang').setMaster('local[*]') # set the configuration
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('../epa-http.txt') # read file line by line to creat RDDs
# use ' ' to split the string
# filter the RDD with valid ip address and valid http return code
# 302 is not a successful return code, thus '-' should be discarded
log_lines_validip = log_lines.filter(lambda x: re.search(regex, x.split(' ')[0]) and x.split(' ')[-1] != '-')
# transform RDDs into pair RDDs ('ip', bytes).
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string
# 141.243.1.172 [29:23:53:25] "GET /Software.html HTTP/1.0" 200 1497
log_pairs = log_lines_validip.map(lambda x: (x.split(' ')[0], int(x.split(' ')[-1])))

# find subnet in which the ip belongs to, use first 6 numbers as the subnet
def subnet(ip):
    point_location = [i for i in range(len(ip)) if ip.startswith('.', i)][1]
    sub = '{}.*.*'.format(ip[0:point_location])
    # sub = '{}.*'.format(ip[0:point_location])
    return sub

# divide ips into different subnets
log_subnet = log_pairs.map(lambda x: (subnet(x[0]), x[1]))

# use spark sql to ensure the deal with log data in even time, rather than received time, i.e. deal with the late data
# creat a spark session
spark = SparkSession \
    .builder \
    .appName('hw1_part4_ChenyeYang') \
    .getOrCreate()

# set the schema of data frame
schema = StructType([
    StructField('subnet', StringType(), True),
    StructField('bytes', IntegerType(), True)])

# create data frame with RDDs and schema
# columns in log_dataframe are ['subnet', 'bytes']
log_dataframe = spark.createDataFrame(log_subnet, schema)

# group ips in one subnet and sum up the bytes in this subnet
# sort by subnet in string ascending order so 100.*.*.* comes before 99.*.*.*
log_csv = log_dataframe.groupBy('subnet').agg(sum('bytes')).sort('subnet', ascending=True)

print('The total number of items: {}'.format(log_csv.count()))
log_csv.show(20, truncate=False)

# save RDD as CSV
f = open('hw1_part4.csv', 'w', newline='')
f_csv = csv.writer(f)
f_csv.writerows(log_csv.collect())
