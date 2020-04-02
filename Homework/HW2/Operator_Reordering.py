# Created by Chenye Yang on 2020/3/31.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import re
import csv
import random
from time import process_time
import matplotlib.pyplot as plt
import numpy as np

# First operator is A, second operator is B
def A_B(log_lines, selectRateA, selectRateB, allowOutput = False):
    # stream pass through operator A
    log_lines_A = log_lines.filter(lambda x: random.random() < selectRateA)
    # stream pass through operator B
    log_lines_B = log_lines_A.filter(lambda x: random.random() < selectRateB)
    if allowOutput:
        print('INPUT--A--B--OUTPUT'.center(40, '*'))
        print('Lines input: {}'.format(log_lines.count()))
        print('Lines after operator A: {}'.format(log_lines_A.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))

# First operator is B, second operator is A
def B_A(log_lines, selectRateA, selectRateB, allowOutput = False):
    # stream pass through operator B
    log_lines_B = log_lines.filter(lambda x: random.random() < selectRateB)
    # stream pass through operator A
    log_lines_A = log_lines_B.filter(lambda x: random.random() < selectRateA)
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
    log_lines = sc.textFile('epa-http.txt')  # read file line by line to create RDDs

    throughput_A_Bs = []
    throughput_B_As = []

    for i in range(100):
        throughput_A_B = []
        throughput_B_A = []

        # Not reordered
        for selectRateB in selectRateBs:
            start = process_time()
            A_B(log_lines, selectRateA, selectRateB, allowOutput=False)
            end = process_time()
            A_B_time = end - start
            # print(A_B_time)
            throughput_A_B.append(A_B_time)

        # Reordered
        for selectRateB in selectRateBs:
            start = process_time()
            B_A(log_lines, selectRateA, selectRateB, allowOutput=False)
            end = process_time()
            B_A_time = end - start
            # print(B_A_time)
            throughput_B_A.append(B_A_time)

        # throughput_A_B = list(map(lambda x: 4448989 / x, throughput_A_B)) # the size of epa-http.txt is 4448989 Byte
        # throughput_B_A = list(map(lambda x: 4448989 / x, throughput_B_A))

        throughput_A_Bs.append(throughput_A_B)
        throughput_B_As.append(throughput_B_A)

    throughput_A_B_mean = np.mean(np.array(throughput_A_Bs), axis=0).tolist()
    throughput_B_A_mean = np.mean(np.array(throughput_B_As), axis=0).tolist()

    maxInAB = max(throughput_A_B_mean)
    minInAB = 0
    throughput_A_B_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_A_B_mean))
    throughput_B_A_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_B_A_mean))


    plt.plot(selectRateBs, throughput_A_B_mean, 'r-', label='Not reordered')
    plt.plot(selectRateBs, throughput_B_A_mean, 'b--', label='Reordered')
    plt.xlabel('Selectivity of B')
    plt.ylabel('Throughput')
    plt.legend()
    plt.title('Selection Reordering')
    plt.show()
