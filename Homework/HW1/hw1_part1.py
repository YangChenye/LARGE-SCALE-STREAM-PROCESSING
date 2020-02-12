# Created by Chenye Yang on 2020/2/11.
# Copyright © 2020 Chenye Yang. All rights reserved.

import pyspark

sc = pyspark.SparkContext()
logfile = sc.textFile('epa-http.txt')
print(logfile.count())
print(logfile.first())