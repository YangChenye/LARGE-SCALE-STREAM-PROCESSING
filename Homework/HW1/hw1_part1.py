# Created by Chenye Yang on 2020/2/11.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark

conf = pyspark.SparkConf().setAppName('hw1_part1_ChenyeYang').setMaster('local[*]')
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('epa-http.txt') # read file line by line to creat RDDs
# transform RDDs into pair RDDs ('ip',bytes). Key: IP address. Value: Bytes served.
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string and select first item as key, last item as value
# last value may be '-', which can not be transferred to int and should be counted as 0
log_ip_bytes_pairs = log_lines.map(lambda x: (x.split(' ')[0], int(x.split(' ')[-1]) if x.split(' ')[-1] != '-' else 0))
# count the total bytes served to same IP addresses, according to keys in ('ip',bytes) pairs
log_ip_total_bytes = log_ip_bytes_pairs.reduceByKey(lambda x, y: x + y)

print(log_ip_total_bytes.count()) # count the number of different IPs
print(log_ip_total_bytes.first()) # check the total bytes served to first IP
