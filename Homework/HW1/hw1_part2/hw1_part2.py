# Created by Chenye Yang on 2020/2/12.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import re
import csv
import argparse

# add command line arguments with parameter K and default number and help
parser = argparse.ArgumentParser(description='Homework 1 Part 2 from Chenye Yang')
parser.add_argument("-K", default=10, help="K is an integer number, to get the top K IPs that were served the most number of bytes")

args = parser.parse_args()
K = args.K

# Make a regular expression for validating an Ip-address
regex = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'

conf = pyspark.SparkConf().setAppName('hw1_part2_ChenyeYang').setMaster('local[*]')
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

## first method to get the top-K ips
# sort the pair RDDs by value, from large to small
log_sort_by_total_bytes = log_ip_total_bytes.sortBy(lambda x: x[1], ascending=False)
# after the sort done, we can directly use take() to "Take the first num elements of the RDD"
print(log_sort_by_total_bytes.take(K))
# [('piankhi.cs.hamptonu.edu', 7267751), ('e659229.boeing.com', 5260561), ('139.121.98.45', 5041738), ......

## second method to get the top-K ips
# or we can ues the top() function to "Get the top N elements from an RDD",
# and choose the key function as the value in pairs.
# this method don't need to use sortBy() function to sort RDDs first
print(log_ip_total_bytes.top(K, lambda x: x[1]))
# [('piankhi.cs.hamptonu.edu', 7267751), ('e659229.boeing.com', 5260561), ('139.121.98.45', 5041738), ......

# save RDD as CSV
f = open('hw1_part2_{}.csv'.format(K), 'w', newline='')
f_csv = csv.writer(f)
f_csv.writerows(log_sort_by_total_bytes.take(K))

