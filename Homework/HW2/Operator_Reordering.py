# Created by Chenye Yang on 2020/3/31.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import re
import csv
import random
from time import process_time_ns
import matplotlib.pyplot as plt

# First operator is A, second operator is B
def A_B(sc, selectRateA, selectRateB, allowOutput = False):
    log_lines = sc.textFile('epa-http.txt')  # read file line by line to create RDDs
    # stream pass through operator A
    log_lines_A = log_lines.filter(lambda x: random.random() <= selectRateA)
    # stream pass through operator B
    log_lines_B = log_lines_A.filter(lambda x: random.random() <= selectRateB)
    if allowOutput:
        print('INPUT--A--B--OUTPUT'.center(40, '*'))
        print('Lines input: {}'.format(log_lines.count()))
        print('Lines after operator A: {}'.format(log_lines_A.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))

# First operator is B, second operator is A
def B_A(sc, selectRateA, selectRateB, allowOutput = False):
    log_lines = sc.textFile('epa-http.txt')  # read file line by line to create RDDs
    # stream pass through operator B
    log_lines_B = log_lines.filter(lambda x: random.random() <= selectRateB)
    # stream pass through operator A
    log_lines_A = log_lines_B.filter(lambda x: random.random() <= selectRateA)
    if allowOutput:
        print('INPUT--B--A--OUTPUT'.center(40, '*'))
        print('Lines input: {}'.format(log_lines.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))
        print('Lines after operator A: {}'.format(log_lines_A.count()))


if __name__ == '__main__':
    selectRateA = 0.5 # the selectivity of operator A is fixed at 0.5
    selectRateBs = [i/100 for i in range(1, 100)] # the selectivity of operator B varies

    conf = pyspark.SparkConf().setAppName('Operator_Reordering').setMaster('local[*]')
    sc = pyspark.SparkContext.getOrCreate(conf=conf)  # creat a spark context object

    throughput_A_B = []
    throughput_B_A = []

    # Not reordered
    for selectRateB in selectRateBs:
        start = process_time_ns()
        A_B(sc, selectRateA, selectRateB, allowOutput=False)
        end = process_time_ns()
        A_B_time = end - start
        throughput_A_B.append(A_B_time)

    # Reordered
    for selectRateB in selectRateBs:
        start = process_time_ns()
        B_A(sc, selectRateA, selectRateB, allowOutput=False)
        end = process_time_ns()
        B_A_time = end - start
        throughput_B_A.append(B_A_time)

    plt.plot(selectRateBs, throughput_A_B, 'r', label='INPUT--A--B--OUTPUT')
    plt.plot(selectRateBs, throughput_B_A, 'b', label='INPUT--B--A--OUTPUT')
    plt.xlabel('Selectivity of B')
    plt.ylabel('Throughput')
    plt.legend()
    plt.title('Selection Reordering')
    plt.show()
