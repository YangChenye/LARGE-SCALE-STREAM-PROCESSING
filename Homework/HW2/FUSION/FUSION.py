# Created by Zhuoyue Xing on 2020/4/02.
# Copyright © 2020 Zhuoyue Xing. All rights reserved.

import pyspark
import random
from time import process_time, sleep
import matplotlib.pyplot as plt
import numpy as np
import os



# First operator is A, second operator is B
def A_B(log_lines, selectRateA, selectRateB,commu_cost, allowOutput = False):
    input_A = log_lines.count()
    # stream pass through operator A
    start = process_time()
    log_lines_A = log_lines.filter(lambda x: random.random() < selectRateA) # only pass through some tuple
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_A):
        sleep(10e-6)
    end = process_time()
    time_A = end - start
    cost_A = time_A / input_A # the computation cost (per tuple) of operator A

    input_B = log_lines_A.count()
    # stream pass through operator B
    start = process_time()
    log_lines_B = log_lines_A.filter(lambda x: random.random() < selectRateB)
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_B):
        sleep(10e-6)
    end = process_time()
    time_B = end - start
    cost_B = time_B / input_B # the computation cost (per tuple) of operator B

    if allowOutput:
        print('Lines after operator A: {}'.format(log_lines_A.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))

    # the computation cost (per tuple) without reordering
    cost_A_B = cost_A + selectRateA * cost_B*(1+1/commu_cost)
    return cost_A_B

# Fusion
def AB(log_lines, selectRateA, selectRateB, commu_cost,allowOutput = False):
    input_A = log_lines.count()
    # stream pass through operator A
    start = process_time()
    log_lines_A = log_lines.filter(lambda x: random.random() < selectRateA)  # only pass through some tuple
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_A):
        sleep(10e-6)
    end = process_time()
    time_A = end - start
    cost_A = time_A / input_A  # the computation cost (per tuple) of operator A

    input_B = log_lines_A.count()
    # stream pass through operator B
    start = process_time()
    log_lines_B = log_lines_A.filter(lambda x: random.random() < selectRateB)
    # we add a small constant number to the cost of processing each tuple, to avoid the arithmetic overflow
    for i in range(input_B):
        sleep(10e-6)
    end = process_time()
    time_B = end - start
    cost_B = time_B / input_B  # the computation cost (per tuple) of operator B

    if allowOutput:
        print('Lines after operator A: {}'.format(log_lines_A.count()))
        print('Lines after operator B: {}'.format(log_lines_B.count()))
    # No communication costs
    cost_AB = cost_A +  cost_B
    return cost_AB


if __name__ == '__main__':
    selectRateA = 0.5 # the selectivity of operator A is fixed at 0.5
    selectRateB = 0.5
    commu_costs = [i/10 for i in range(1, 50)] # the selectivity of operator B varies
    allowOutput = True # allow printing of number of tuples processed
    runs = 50 # run the system for many times to smooth the throughput curve
    cwd = os.getcwd()
    conf = pyspark.SparkConf().setAppName('FUSION').setMaster('local[*]')
    sc = pyspark.SparkContext.getOrCreate(conf=conf)  # creat a spark context object
    log_lines = sc.textFile(cwd + "\\epa-http.txt")  # read file line by line to create RDDs

    # store the throughput of different runs
    throughput_A_Bs = []
    throughput_ABs = []
    # run the system for many times and calculate the average
    for i in range(runs):
        # the throughput of the systems with different select rate
        throughput_A_B = []
        throughput_AB = []

        # Not fused
        for commu_cost in commu_costs:
            if allowOutput:
                print('INPUT--A--B--OUTPUT'.center(40, '*'))
                print('Lines input: {}'.format(log_lines.count()))
            # the cost in not reordered system, per tuple
            cost_A_B = A_B(log_lines, selectRateA, selectRateB, commu_cost, allowOutput=allowOutput)
            throughput_A_B.append(cost_A_B)

        # Fused
        for commu_cost in commu_costs:
            if allowOutput:
                print('INPUT--B--A--OUTPUT'.center(40, '*'))
                print('Lines input: {}'.format(log_lines.count()))
            # the cost in reordered system, per tuple
            cost_AB = AB(log_lines, selectRateA, selectRateB, commu_cost, allowOutput=allowOutput)
            throughput_AB.append(cost_AB)

        # throughput: how many tuples processed per second
        throughput_A_B = list(map(lambda x: 1 / x, throughput_A_B))
        throughput_AB = list(map(lambda x: 1 / x, throughput_AB))

        throughput_A_Bs.append(throughput_A_B)
        throughput_ABs.append(throughput_AB)

    # average the throughput to smooth the curve
    throughput_A_B_mean = np.mean(np.array(throughput_A_Bs), axis=0).tolist()
    throughput_AB_mean = np.mean(np.array(throughput_ABs), axis=0).tolist()

    # normalization
    maxInAB = max(throughput_A_B_mean)
    minInAB = 0
    throughput_A_B_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_A_B_mean))
    throughput_AB_mean = list(map(lambda x: (x - minInAB) / (maxInAB - minInAB), throughput_AB_mean))

    # visualization
    plt.plot(commu_costs, throughput_A_B_mean, 'r-', label='Not fused')
    plt.plot(commu_costs, throughput_AB_mean, 'b--', label='Fused')
    plt.xlabel('Operator cost / communication cost')
    plt.ylabel('Throughput')
    plt.legend()
    plt.title('FUSION')
    plt.show()