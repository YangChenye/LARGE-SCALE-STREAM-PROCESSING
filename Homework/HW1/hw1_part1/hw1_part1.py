# Created by Chenye Yang on 2020/2/11.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import re
import csv

# Make a regular expression for validating an Ip-address
regex = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'

conf = pyspark.SparkConf().setAppName('hw1_part1_ChenyeYang').setMaster('local[*]')
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('../epa-http.txt') # read file line by line to creat RDDs
# use ' ' to split the string
# filter the RDD with valid ip address and valid http return code
# 302 is not a successful return code, thus '-' should be discarded
log_lines_validip = log_lines.filter(lambda x: re.search(regex, x.split(' ')[0]) and x.split(' ')[-1] != '-')
# transform RDDs into pair RDDs ('ip',bytes). Key: IP address. Value: Bytes served.
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string and select first item as key, last item as value
log_ip_bytes_pairs = log_lines_validip.map(lambda x: (x.split(' ')[0], int(x.split(' ')[-1])))
# count the total bytes served to same IP addresses, according to keys in ('ip',bytes) pairs
log_ip_total_bytes = log_ip_bytes_pairs.reduceByKey(lambda x, y: x + y)
# sort by IP in string ascending order so 100.1.2.3 comes before 99.2.3.5
log_sort_by_ip = log_ip_total_bytes.sortByKey(ascending=True)
# save RDD as CSV
f = open('hw1_part1.csv', 'w', newline='')
f_csv = csv.writer(f)
f_csv.writerows(log_sort_by_ip.collect())

print(log_lines_validip.count())
print(log_sort_by_ip.count()) # count the number of different IPs
print(log_sort_by_ip.take(20)) # check the total bytes served to first IP
