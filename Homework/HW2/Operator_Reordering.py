# Created by Chenye Yang on 2020/3/31.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import re
import csv
import random


# First operator is A, second operator is B
def A_B(selectRateA, selectRateB, iteration):
    throughput = []
    for i in range(iteration):
        conf = pyspark.SparkConf().setAppName('hw1_part1_ChenyeYang').setMaster('local[*]')
        sc = pyspark.SparkContext(conf=conf) # creat a spark context object
        log_lines = sc.textFile('../epa-http.txt') # read file line by line to create RDDs

        # stream pass through operator A
        log_lines_A = log_lines.filter(lambda x: random.random() > selectRateA)
        # stream pass through operator B
        log_lines_B = log_lines_A.filter(lambda x: random.random() > selectRateB)







if __name__ == '__main__':
    selectRateA = 0.5 # the selectivity of operator A is fixed at 0.5
    selectRateB = 0.1 # the selectivity of operator B varies
