# Created by Chenye Yang on 2020/3/31.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark
import random
from time import process_time, sleep
import matplotlib.pyplot as plt
import numpy as np

# First operator is A, second operator is B
def A_B(log_lines, selectRateA, selectRateB, allowOutput = False):
    input_A = log_lines.count()
    # stream pass through operator A
    start = process_time()
    log_lines_A = log_lines.filter(lambda x: random.random() < selectRateA) # only pass through some tuple
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_A):
        sleep(1e-6)
    end = process_time()
    time_A = end - start
    cost_A = time_A / input_A # the computation cost (per tuple) of operator A

    input_B = log_lines_A.count()
    # stream pass through operator B
    start = process_time()
    log_lines_B = log_lines_A.filter(lambda x: random.random() < selectRateB)
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_B):
        sleep(1e-6)
    end = process_time()
    time_B = end - start
    cost_B = time_B / input_B # the computation cost (per tuple) of operator B

    if allowOutput:
        print('Lines after operator A: {}'.format(log_lines_A.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))

    # the computation cost (per tuple) without reordering
    cost_A_B = cost_A + selectRateA * cost_B
    return cost_A_B

# First operator is B, second operator is A
def B_A(log_lines, selectRateA, selectRateB, allowOutput = False):
    input_B = log_lines.count()
    # stream pass through operator B
    start = process_time()
    log_lines_B = log_lines.filter(lambda x: random.random() < selectRateB)
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_B):
        sleep(1e-6)
    end = process_time()
    time_B = end - start
    cost_B = time_B / input_B # the computation cost (per tuple) of operator B

    input_A = log_lines_B.count()
    # stream pass through operator A
    start = process_time()
    log_lines_A = log_lines_B.filter(lambda x: random.random() < selectRateA)
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_A):
        sleep(1e-6)
    end = process_time()
    time_A = end - start
    cost_A = time_A / input_A # the computation cost (per tuple) of operator A

    if allowOutput:
        print('Lines after operator B: {}'.format(log_lines_B.count()))
        print('Lines after operator A: {}'.format(log_lines_A.count()))

    # the computation cost (per tuple) with reordering
    cost_B_A = cost_B + selectRateB * cost_A
    return cost_B_A


if __name__ == '__main__':
    selectRateA = 0.5 # the selectivity of operator A is fixed at 0.5
    selectRateBs = [i/100 for i in range(1, 100)] # the selectivity of operator B varies
    allowOutput = False # allow printing of number of tuples processed
    runs = 50 # run the system for many times to smooth the throughput curve

    conf = pyspark.SparkConf().setAppName('Operator_Reordering').setMaster('local[*]')
    sc = pyspark.SparkContext.getOrCreate(conf=conf)  # creat a spark context object
    log_lines = sc.textFile('epa-http.txt')  # read file line by line to create RDDs

    # store the throughput of different runs
    throughput_A_Bs = []
    throughput_B_As = []

    # run the system for many times and calculate the average
    for i in range(runs):
        # the throughput of the systems with different select rate
        throughput_A_B = []
        throughput_B_A = []

        # Not reordered
        for selectRateB in selectRateBs:
            if allowOutput:
                print('INPUT--A--B--OUTPUT'.center(40, '*'))
                print('Lines input: {}'.format(log_lines.count()))
            # the cost in not reordered system, per tuple
            cost_A_B = A_B(log_lines, selectRateA, selectRateB, allowOutput=allowOutput)
            throughput_A_B.append(cost_A_B)

        # Reordered
        for selectRateB in selectRateBs:
            if allowOutput:
                print('INPUT--B--A--OUTPUT'.center(40, '*'))
                print('Lines input: {}'.format(log_lines.count()))
            # the cost in reordered system, per tuple
            cost_B_A = B_A(log_lines, selectRateA, selectRateB, allowOutput=allowOutput)
            throughput_B_A.append(cost_B_A)

        # throughput: how many tuples processed per second
        throughput_A_B = list(map(lambda x: 1 / x, throughput_A_B))
        throughput_B_A = list(map(lambda x: 1 / x, throughput_B_A))

        throughput_A_Bs.append(throughput_A_B)
        throughput_B_As.append(throughput_B_A)

    # average the throughput to smooth the curve
    throughput_A_B_mean = np.mean(np.array(throughput_A_Bs), axis=0).tolist()
    throughput_B_A_mean = np.mean(np.array(throughput_B_As), axis=0).tolist()

    # normalization
    maxInAB = max(throughput_A_B_mean)
    minInAB = 0
    throughput_A_B_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_A_B_mean))
    throughput_B_A_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_B_A_mean))

    # visualization
    plt.plot(selectRateBs, throughput_A_B_mean, 'r-', label='Not reordered')
    plt.plot(selectRateBs, throughput_B_A_mean, 'b--', label='Reordered')
    plt.xlabel('Selectivity of B')
    plt.ylabel('Throughput')
    plt.legend()
    plt.title('Selection Reordering')
    plt.show()
