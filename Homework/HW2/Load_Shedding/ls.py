import pyspark
import re
import csv
import random
from time import process_time, sleep
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance


def sampling(log_lines, selectRate):
    # stream pass through sampling operator
    log_lines_sample = log_lines.filter(lambda x: random.random() < selectRate)
    return log_lines_sample


def aggregation(log_lines):
    # stream pass through aggregate operator
    byte_count = log_lines.reduceByKey(lambda x, y: x+y)
    # print(byte_count.sortByKey().top(10))
    return byte_count.sortByKey().collect()


def distance(list_A, list_B):
    # print(list_A, list_B)
    distribution_A = []
    weight_A = []
    for item in list_A:
        distribution_A.append(item[0])
        weight_A.append(item[1])

    distribution_B = []
    weight_B = []
    for item in list_B:
        distribution_B.append(item[0])
        weight_B.append(item[1])

    # print(distribution_A, distribution_B, weight_A, weight_B)

    return wasserstein_distance(distribution_A, distribution_B, weight_A, weight_B)


if __name__ == '__main__':

    selectivity = [i/100 for i in range(1, 99)]

    conf = pyspark.SparkConf().setAppName('Load_shedding').setMaster('local[*]')
    sc = pyspark.SparkContext.getOrCreate(conf=conf)  # creat a spark context object
    log_lines = sc.textFile('epa-http.txt')  # read file line by line to create RDDs
    cleaned_lines = log_lines.filter(lambda x: (x[-1] != "-"))
    bytes_per_line = cleaned_lines.map(lambda x: (int(x.split(" ")[-1])//10000, 1))

    throughput_time = []
    error = []
    for select in selectivity:
        # compute delay time
        bytes_per_line_sampled = sampling(bytes_per_line, select)
        start = process_time()
        reduced_list = aggregation(bytes_per_line_sampled)
        for i in range(bytes_per_line_sampled.count()):
            sleep(1e-6)
        end = process_time()
        t_time = end - start
        # print(bytes_per_line_sampled.count(), t_time, select)
        throughput_time.append(t_time/bytes_per_line_sampled.count())
        # compute accuracy
        base_list = aggregation(bytes_per_line)
        error.append(distance(base_list, reduced_list))

    # print(throughput_time)
    max_time = max(throughput_time)
    min_time = min(throughput_time)

    # compute throughput
    throughput = []
    for i in range(len(selectivity)):
        throughput.append((throughput_time[i]-min_time)/(max_time-min_time))

    # compute accuracy
    accuracy = []
    for i in range(len(selectivity)):
        accuracy.append((1-error[i]))

    plt.plot(selectivity, accuracy, 'r-', label='Accuracy')
    plt.plot(selectivity, throughput, 'b-', label='Throughput')
    plt.xlabel('Selectivity')
    plt.ylabel('Value')
    plt.legend()
    plt.title('Load Shedding')
    plt.show()
