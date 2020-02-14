# Created by Chenye Yang on 2020/2/12.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark

conf = pyspark.SparkConf().setAppName('hw1_part2_ChenyeYang').setMaster('local[*]')
sc = pyspark.SparkContext(conf=conf) # creat a spark context object
log_lines = sc.textFile('epa-http.txt') # read file line by line to creat RDDs
# transform RDDs into pair RDDs ('ip',bytes). Key: IP address. Value: Bytes served.
# use map() because one input RDD corresponding to one output RDD
# use ' ' to split the string and select first item as key, last item as value
# last value may be '-', which can not be transferred to int and should be counted as 0
log_ip_bytes_pairs = log_lines.map(lambda x: (x.split(' ')[0], int(x.split(' ')[-1]) if x.split(' ')[-1] != '-' else 0))
# count the total bytes served to same IP addresses, according to keys in ('ip',bytes) pairs
log_ip_total_bytes = log_ip_bytes_pairs.reduceByKey(lambda x, y: x + y)

## first method to get the top-K ips
# sort the pair RDDs by value, from large to small
log_sort_by_total_bytes = log_ip_total_bytes.sortBy(lambda x: x[1], ascending=False)
# after the sort done, we can directly use take() to "Take the first num elements of the RDD"
K = 10
print(log_sort_by_total_bytes.take(K))
# [('piankhi.cs.hamptonu.edu', 7267751), ('e659229.boeing.com', 5260561), ('139.121.98.45', 5041738), ......

## second method to get the top-K ips
# or we can ues the top() function to "Get the top N elements from an RDD",
# and choose the key function as the value in pairs.
# this method don't need to use sortBy() function to sort RDDs first
print(log_ip_total_bytes.top(K, lambda x: x[1]))
# [('piankhi.cs.hamptonu.edu', 7267751), ('e659229.boeing.com', 5260561), ('139.121.98.45', 5041738), ......
